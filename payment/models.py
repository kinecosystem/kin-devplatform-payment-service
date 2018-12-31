import json
from collections import namedtuple
from datetime import datetime
from schematics import Model
from schematics.types import StringType, IntType, DateTimeType, BooleanType
from kin.transactions import NATIVE_ASSET_TYPE, SimplifiedTransaction
from kin import decode_transaction
from kin import KinErrors
from .errors import PaymentNotFoundError, ParseError, OrderNotFoundError, TransactionMismatch
from .redis_conn import redis_conn
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
    # XXX validate should raise 400 error


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
    def compare_attr(attr1, attr2, attr_name):
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
            raise TransactionMismatch('Transaction could not be deocded')
        memo_parts = decoded_tx.memo.split('-')
        if len(memo_parts) != 3:
            raise TransactionMismatch('Unexpected memo')
        self.compare_attr(memo_parts[1], self.app_id, 'App id')
        self.compare_attr(memo_parts[2], self.order_id, 'Order id')
        self.compare_attr(decoded_tx.source, self.source, 'Source account')
        self.compare_attr(decoded_tx.operation.destination, self.destination, 'Destination account')
        self.compare_attr(decoded_tx.operation.amount, self.amount, 'Amount')

    def whitelist(self) -> str:
        """Sign and return a transaction to whitelist it"""
        # Get app hot wallet account
        # Fix for circular imports
        # https://stackoverflow.com/questions/1250103/attributeerror-module-object-has-no-attribute
        from .blockchain import get_sdk
        app_seed = APP_SEEDS.get(self.app_id).our
        with get_sdk(app_seed) as hot_account:
            return hot_account.write_sdk.whitelist_transaction({'envelope': self.xdr,
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
        t.timestamp = DateTimeType().from_string(data.timestamp)
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
    def get(cls, payment_id):
        data = redis_conn.get(cls._key(payment_id))
        if not data:
            raise PaymentNotFoundError('payment {} not found'.format(payment_id))
        return Payment(json.loads(data))

    @classmethod
    def _key(cls, id):
        return 'payment:{}'.format(id)

    def save(self):
        redis_conn.set(self._key(self.id),
                       json.dumps(self.to_primitive()),
                       ex=self.PAY_STORE_TIME)


class Watcher():
    @classmethod
    def add(cls, service_id, address, order_id):
        # Add an order to the address, redis will not add the same order_id twice.
        redis_conn.sadd(cls.get_name(service_id, address), order_id)

    @classmethod
    def get_name(cls, service_id, address):
        return service_id + ':' + address

    @classmethod
    def remove(cls, service_id, address, order_id):
        # Remove an order from the address, if the address has no more orders it will be deleted
        reply = redis_conn.srem(service_id + ':' + address, order_id)
        # reply is the number of items changed, expected to be 1
        if reply != 1:
            raise OrderNotFoundError('Wallet {} did not watch order {}'.format(address, order_id))

    @classmethod
    def get_all_addresses(cls):
        # Get all keys that are addresses
        data = redis_conn.keys("*:G*")
        # Create a list of addresses by decoding the  byte strings and taking everything after ":"
        addresses = [key.decode().split(':')[1] for key in data]
        return addresses

    @classmethod
    def get_subscribed(cls, address):
        """get services interested in an address."""
        # Get all keys that are addresses
        data = redis_conn.keys("*:{}".format(address))
        # Create a list of addresses by decoding the  byte strings and taking everything before ":"
        services = [key.decode().split(':')[0] for key in data]
        return services


class Service:
    # TODO: add endpoint to create new service
    SERVICE_PREFIX = 'service:'

    @classmethod
    def get(cls, service_id):
        data = redis_conn.get(cls.SERVICE_PREFIX + service_id)
        if data is None:
            return data
        # Redis returns data in bytestrings, need to decode it.
        return data.decode()

    @classmethod
    def new(cls, service_id, callback_url):
        reply = redis_conn.set(cls.SERVICE_PREFIX + service_id, callback_url)
        # reply is True|False
        return reply




class TransactionRecord(ModelWithStr):
    to_address = StringType(serialized_name='to', required=True)
    from_address = StringType(serialized_name='from', required=True)
    transaction_hash = StringType(required=True)
    asset_code = StringType()
    asset_issuer = StringType()
    paging_token = StringType(required=True)
    type = StringType(required=True)


class CursorManager:
    @classmethod
    def save(cls, cursor):
        redis_conn.set(cls._key(), cursor)
        return cursor

    @classmethod
    def get(cls):
        cursor = redis_conn.get(cls._key())
        return cursor.decode('utf8') if cursor else None

    @classmethod
    def _key(cls):
        return 'cursor'

