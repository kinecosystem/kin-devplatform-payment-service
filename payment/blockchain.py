from kin.errors import AccountNotFoundError, HorizonError, translate_horizon_error
from kin.transactions import SimplifiedTransaction
from kin import KinClient
from kin.account import KinAccount
from kin.config import MAX_RECORDS_PER_REQUEST

from .models import Payment, Wallet, TransactionRecord
from .log import get as get_log
from .errors import WalletNotFoundError

from typing import Dict, List

log = get_log("Blockchain")


class Blockchain:
    def __init__(self, account: KinAccount, minimum_fee: int):
        self.account = account
        self.minimum_fee = minimum_fee

    async def create_wallet(self, public_address: str) -> str:
        """create a wallet."""
        log.info('creating wallet', public_address=public_address)
        tx_id = await self.account.create_account(public_address, 0, self.minimum_fee)
        log.info('create wallet transaction', tx_id=tx_id)
        return tx_id

    async def pay_to(self, public_address: str, amount: int, payment_id: str) -> str:
        """send kins to an address."""
        log.info('sending kin to', address=public_address)
        return await self.account.send_kin(public_address, amount, fee=self.minimum_fee, memo_text=payment_id)


class BlockchainManager:

    def __init__(self, client: KinClient, minimum_fee: int):
        self.client = client
        self.minimum_fee = minimum_fee
        self.accounts: Dict[str, Blockchain] = {}

    def add_account(self, seed: str, channels: List[str], app_id: str):
        """Add a new 'Blockchain' instance to the dict"""
        kin_account = self.client.kin_account(seed, channels, app_id)
        self.accounts[seed] = Blockchain(kin_account, self.minimum_fee)

    async def get_wallet(self, public_address: str) -> Wallet:
        try:
            data = await self.client.get_account_data(public_address)
            return Wallet.from_blockchain(data)
        except AccountNotFoundError:
            raise WalletNotFoundError('wallet %s not found' % public_address)

    async def get_transaction_data(self, tx_id) -> SimplifiedTransaction:
        return await self.client.get_transaction_data(tx_id)

    async def get_payment_data(self, tx_id) -> Payment:
        return Payment.from_blockchain(await self.get_transaction_data(tx_id))

    async def get_last_ledger_seq(self):
        return (await self.client.horizon.ledgers(order='desc', limit=1))['_embedded']['records'][0]['sequence']

    async def get_txs_per_ledger(self, ledger_seq: int, paging_token=None) -> List[Dict]:
        txs = []
        while True:
            try:
                result = (await self.client.horizon.ledger_transactions(ledger_seq, limit=MAX_RECORDS_PER_REQUEST,
                                                                        cursor=paging_token))['_embedded']['records']
                txs.extend(result)

                if len(result) != MAX_RECORDS_PER_REQUEST:  # There are no more txs left in the ledger
                    return txs

                paging_token = result[-1]['paging_token']

            except HorizonError as e:
                raise translate_horizon_error(e)



