import json
from collections import namedtuple
from datetime import datetime

from aioredis import Redis
from schematics import Model
from schematics.types import StringType, IntType, DateTimeType, BooleanType
from kin.transactions import NATIVE_ASSET_TYPE, SimplifiedTransaction
from kin import decode_transaction
from kin import KinErrors
from kin import Keypair

from .errors import PaymentNotFoundError, ParseError, OrderNotFoundError, TransactionMismatch
from .log import get as get_log
from .config import APP_SEEDS

log = get_log()


Memo = namedtuple('Memo', ['app_id', 'payment_id'])


class ModelWithStr(Model):
    def __str__(self):
        return json.dumps(self.to_primitive())

    def __repr__(self):
        return str(self)


class WalletRequest(ModelWithStr):
    wallet_address = StringType()
    app_id = StringType()
    id = StringType()
    callback = StringType()  # a webhook to call when a wallet creation is complete


class Wallet(ModelWithStr):
    wallet_address = StringType()
    kin_balance = IntType()
    native_balance = IntType()
    id = StringType()

    @classmethod
    def from_blockchain(cls, data):
        wallet = Wallet()
        wallet.wallet_address = data.id
        kin_balance = next(
            (coin.balance for coin in data.balances
             if coin.asset_type == NATIVE_ASSET_TYPE))
        wallet.kin_balance = None if kin_balance is None else int(kin_balance)
        return wallet


class PaymentRequest(ModelWithStr):
    amount = IntType()
    app_id = StringType()
    is_external = BooleanType()
    recipient_address = StringType()
    sender_address = StringType()
    id = StringType()
    callback = StringType()  # a webhook to call when a payment is complete


class WhitelistRequest(ModelWithStr):
    order_id = StringType()
    source = StringType()
    destination = StringType()
    amount = IntType()
    xdr = StringType()
    network_id = StringType()
    app_id = StringType()

    @staticmethod
    def _compare_attr(attr1, attr2, attr_name):
        if attr1 != attr2:
            raise TransactionMismatch('{attr_name}: {attr1} does not match expected {attr_name}: {attr2}'.
                                      format(attr_name=attr_name,
                                             attr1=attr1,
                                             attr2=attr2))

    def verify_transaction(self):
        """Verify that the encoded transaction matches our expectations"""
        try:
            decoded_tx = decode_transaction(self.xdr, self.network_id)
        except Exception as e:
            if isinstance(e, KinErrors.CantSimplifyError):
                raise TransactionMismatch('Unexpected transaction')
            log.error('Couldn\'t decode tx with xdr: {}'.format(self.xdr))
            raise TransactionMismatch('Transaction could not be decoded')
        if decoded_tx.memo is None:
            raise TransactionMismatch('Unexpected memo')
        memo_parts = decoded_tx.memo.split('-')
        if len(memo_parts) != 3:
            raise TransactionMismatch('Unexpected memo')
        self._compare_attr(memo_parts[1], self.app_id, 'App id')
        self._compare_attr(memo_parts[2], self.order_id, 'Order id')
        self._compare_attr(decoded_tx.source, self.source, 'Source account')
        self._compare_attr(decoded_tx.operation.destination, self.destination, 'Destination account')
        self._compare_attr(decoded_tx.operation.amount, self.amount, 'Amount')

    def whitelist(self, bc_manager) -> str:
        """Sign and return a transaction to whitelist it"""
        app_seed = APP_SEEDS.get(self.app_id).our
        # Get app hot wallet account
        hot_account = bc_manager.accounts[app_seed].account
        return hot_account.whitelist_transaction({'envelope': self.xdr,
                                                  'network_id': self.network_id})


class Payment(ModelWithStr):
    PAY_STORE_TIME = 500
    id = StringType()
    app_id = StringType()
    transaction_id = StringType()
    recipient_address = StringType()
    sender_address = StringType()
    amount = IntType()
    timestamp = DateTimeType(default=datetime.utcnow())

    @classmethod
    def from_blockchain(cls, data: SimplifiedTransaction):
        t = Payment()
        t.id = cls.parse_memo(data.memo).payment_id
        t.app_id = cls.parse_memo(data.memo).app_id
        t.transaction_id = data.id
        t.sender_address = data.source
        t.recipient_address = data.operation.destination
        t.amount = int(data.operation.amount)
        t.timestamp = datetime.strptime(data.timestamp, '%Y-%m-%dT%H:%M:%SZ')  # 2018-11-12T06:45:40Z
        return t

    @classmethod
    def parse_memo(cls, memo):
        try:
            version, app_id, payment_id = memo.split('-')
            return Memo(app_id, payment_id)
        except Exception:
            raise ParseError

    @classmethod
    def create_memo(cls, app_id, payment_id):
        """serialize args to the memo string."""
        return '1-{}-{}'.format(app_id, payment_id)

    @classmethod
    async def get(cls, payment_id, redis_conn: Redis):
        data = await redis_conn.get(cls._key(payment_id))
        if not data:
            raise PaymentNotFoundError('payment {} not found'.format(payment_id))
        return Payment(json.loads(data.decode()))

    @classmethod
    def _key(cls, id):
        return 'payment:{}'.format(id)

    async def save(self, redis_conn: Redis):
        await redis_conn.set(self._key(self.id),
                       json.dumps(self.to_primitive()),
                       expire=self.PAY_STORE_TIME)

class TransactionRecord(ModelWithStr):
    to_address = StringType(serialized_name='to', required=True)
    from_address = StringType(serialized_name='from', required=True)
    transaction_hash = StringType(required=True)
    asset_type = StringType()
    paging_token = StringType(required=True)
    type = StringType(required=True)
