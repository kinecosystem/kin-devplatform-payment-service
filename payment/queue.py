import asyncio

import kin
from aiohttp import ClientSession
from aioredis import Redis
from aioredis_lock import RedisLock, LockTimeoutError

from . import config
from .errors import PaymentNotFoundError
from .log import get as get_log
from .models import Payment, PaymentRequest, WalletRequest, Wallet
from .statsd import statsd
from .blockchain import BlockchainManager
from kin.blockchain.keypair import Keypair


log = get_log("Enqueuer")

CALLBACK_RETIRES = 5
CALLBACK_BACKOFF = 0.2

class Enqueuer:
    def __init__(self, bc_manager: BlockchainManager, redis_conn: Redis):
        self.bc_manager = bc_manager
        self.session: ClientSession = bc_manager.client.horizon._session
        self.redis_conn = redis_conn

    def enqueue_send_payment(self, payment_request: PaymentRequest):
        statsd.inc_count('transaction.enqueue',
                         payment_request.amount,
                         tags=['app_id:%s' % payment_request.app_id])
        asyncio.create_task(self.pay_and_callback(payment_request))

    def enqueue_create_wallet(self, wallet_request: WalletRequest):
        statsd.increment('wallet_creation.enqueue',
                         tags=['app_id:%s' % wallet_request.app_id])

        asyncio.create_task(self.create_wallet_and_callback(wallet_request))

    def __enqueue_callback(self, callback: str, app_id: str, objekt: str, state: str, action: str, value: dict):
        statsd.increment('callback.enqueue',
                         tags=['app_id:%s' % app_id,
                               'object:%s' % objekt,
                               'state:%s' % state,
                               'action:%s' % action])

        asyncio.create_task(self.call_callback(callback, app_id, objekt=objekt,
                                               state=state, action=action, value=value))

    def enqueue_wallet_callback(self, wallet_request: WalletRequest, value: Wallet):
        self.__enqueue_callback(
            callback=wallet_request.callback,
            app_id=wallet_request.app_id,
            objekt='wallet',
            state='success',
            action='create',
            value=value.to_primitive())

    def enqueue_payment_callback(self, callback: str, wallet_address: str, value: Payment):
        self.__enqueue_callback(
            callback=callback,
            app_id=value.app_id,
            objekt='payment',
            state='success',
            action='send' if wallet_address == value.sender_address else 'receive',
            value=value.to_primitive())

    def enqueue_payment_failed_callback(self, request: PaymentRequest, reason: str):
        self.__enqueue_callback(
            callback=request.callback,
            app_id=request.app_id,
            objekt='payment',
            state='fail',
            action='send',
            value={'id': request.id, 'reason': reason})

    def enqueue_wallet_failed_callback(self, request: WalletRequest, reason: str):
        self.__enqueue_callback(
            callback=request.callback,
            app_id=request.app_id,
            objekt='wallet',
            state='fail',
            action='create',
            value={'id': request.id, 'reason': reason})

    async def call_callback(self, callback: str, app_id: str, objekt: str, state: str, action: str, value: dict):

        async def retry_callback(callback: str, payload: dict):
            for i in range(CALLBACK_RETIRES):
                try:
                    # The kin client sets x-www-form-urlencoded as the content type, so override it just for this request
                    async with self.session.post(callback, json=payload, raise_for_status=True,
                                                 headers={'Content-Type': 'application/json'}) as resp:
                        return await resp.json()
                except:
                    if i == 4:
                        raise

                await asyncio.sleep(CALLBACK_BACKOFF)

        payload = {
            'object': objekt,
            'state': state,
            'action': action,
            'value': value,
        }

        tags = ['app_id:%s' % app_id,
                'object:%s' % objekt,
                'state:%s' % state,
                'action:%s' % action]
        try:
            response = await retry_callback(callback, payload)
            log.info('callback response', response=response, payload=payload)
            statsd.increment('callback.success', tags=tags)
        except Exception as e:
            log.error('callback failed', error=e, payload=payload)
            statsd.increment('callback.failed', tags=tags)

    async def pay_and_callback(self, payment_request: PaymentRequest):
        """lock, try to pay and callback."""
        log.info('pay_and_callback received', payment_request=payment_request)
        try:
            async with RedisLock(self.redis_conn, '_lock_payment:{}'.format(payment_request.id),
                                 timeout=60, wait_timeout=0):
                try:
                    payment = await self.pay(payment_request)
                except Exception as e:
                    self.enqueue_payment_failed_callback(payment_request, str(e))
                else:
                    self.enqueue_payment_callback(payment_request.callback, payment.sender_address, payment)

        except LockTimeoutError:
            log.warning(f'Got a repeating request for payment in progress: {payment_request.id}')
            pass

    async def create_wallet_and_callback(self, wallet_request: WalletRequest):
        log.info('create_wallet_and_callback received', wallet_request=wallet_request.__dict__)

        # Use the app's wallet to create the user's wallet
        selected_seed = config.APP_SEEDS.get(wallet_request.app_id).our
        try:
            await self.bc_manager.accounts[selected_seed].create_wallet(wallet_request.wallet_address)

        except kin.KinErrors.AccountExistsError as e:
            statsd.increment('wallet.exists', tags=['app_id:%s' % wallet_request.app_id])
            log.info('wallet already exists - ok', public_address=wallet_request.wallet_address)
            self.enqueue_wallet_failed_callback(wallet_request, "account exists")

        except Exception as e:
            statsd.increment('wallet.failed', tags=['app_id:%s' % wallet_request.app_id])
            self.enqueue_wallet_failed_callback(wallet_request, str(e))

        else:
            statsd.increment('wallet.created', tags=['app_id:%s' % wallet_request.app_id])
            wallet = await self.bc_manager.get_wallet(wallet_request.wallet_address)  # TODO: maybe delete this? just an extra web request
            wallet.id = wallet_request.id  # XXX id is required in webhook
            self.enqueue_wallet_callback(wallet_request, wallet)

    async def pay(self, payment_request: PaymentRequest):
        """pays only if not already paid."""
        try:
            payment = await Payment.get(payment_request.id, self.redis_conn)
            log.info('payment is already complete - not double spending', payment=payment)
            return payment
        except PaymentNotFoundError:
            pass

        log.info('trying to pay', payment_id=payment_request.id)

        try:
            sender_public_address = payment_request.sender_address
            our_seed = config.APP_SEEDS.get(payment_request.app_id).our
            joined_seed = config.APP_SEEDS.get(payment_request.app_id).joined
            selected_seed = our_seed if Keypair.address_from_seed(our_seed) == sender_public_address else joined_seed
            paying_account = self.bc_manager.accounts[selected_seed]
            tx_id = await paying_account.pay_to(payment_request.recipient_address,
                                                payment_request.amount,
                                                payment_request.id)

            log.info('paid transaction', tx_id=tx_id, payment_id=payment_request.id)
            statsd.inc_count('transaction.paid',
                             payment_request.amount,
                             tags=['app_id:%s' % payment_request.app_id])
        except Exception as e:
            log.exception('failed to pay transaction', error=e, payment_id=payment_request.id)
            statsd.increment('transaction.failed',
                             tags=['app_id:%s' % payment_request.app_id])
            raise

        payment = Payment({'id': payment_request.id,
                           'app_id': payment_request.app_id,
                           'transaction_id': tx_id,
                           'recipient_address': payment_request.recipient_address,
                           'sender_address': paying_account.account.get_public_address(),
                           'amount': payment_request.amount})
        await payment.save(self.redis_conn)

        log.info('payment complete - submit back to callback payment.callback', payment=payment)

        return payment
