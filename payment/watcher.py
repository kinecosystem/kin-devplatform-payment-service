import time
import asyncio
from concurrent.futures import ProcessPoolExecutor

from kin.transactions import SimplifiedTransaction, RawTransaction, OperationTypes
from kin.errors import CantSimplifyError, ResourceNotFoundError

from .blockchain import BlockchainManager
from .queue import Enqueuer
from .models import Payment
from .log import get as get_log
from .statsd import statsd

from typing import Set, Dict , List

log = get_log('Watcher')
WAIT_WHEN_FOUND = 4.5  # How long to wait until querying about the next ledger when we got a response
WAIT_WHEN_NOT_FOUND = 1  # How long to wait until querying about the next ledger when we got a 404
ORDER_EXPIRY = 70  # How long before we stop watching an order
RETRIES_BEFORE_EXCEPTION = 5  # Will continue retrying, but will only raise an error after n retries


class PaymentWatcher:
    def __init__(self, enqueuer: Enqueuer, bc_manager: BlockchainManager, webhook: str, redis_conn):
        self.enqueuer = enqueuer
        self.webhook = webhook
        self.bc_manager = bc_manager
        self.watched_addresses: Dict[str, Set[str]] = {}  # Map address:orders
        self.task: asyncio.Task = None
        self.executor = ProcessPoolExecutor()
        self.loop = asyncio.get_running_loop()
        self._expiration_tasks: Dict[str, asyncio.TimerHandle] = {}

    def watch(self, address: str, order: str):
        """Start watching an order for an address"""
        if address in self.watched_addresses:
            self.watched_addresses[address].add(order)
        else:
            self.watched_addresses[address] = {order}
        self._expiration_tasks[order] = self.loop.call_later(ORDER_EXPIRY, self.remove, address, order)
        log.debug(f'Added order: {order} to {address} watch list')

    def remove(self, address, order):
        """Remove an order from an address, and the address from the list if possible"""
        self.watched_addresses[address].remove(order)
        log.debug(f'Removed {order} from {address} watch list')
        if len(self.watched_addresses[address]) == 0:
            del self.watched_addresses[address]
            log.debug(f'Removed {address} from watch list')

        self._expiration_tasks[order].cancel()

    def start(self):
        log.info(f'Starting payment watcher')
        self.task = asyncio.create_task(self.worker())

    def stop(self):
        log.info(f'Stopping payment watcher')
        if self.task is not None:
            self.task.cancel()
        # This only happens when closing the server,
        # so its fine to block until we close the processes
        self.executor.shutdown(wait=True)

    async def get_last_ledger_sequence(self):
        seq = await self.bc_manager.get_last_ledger_seq()
        log.info('got ledger seq from horizon', seq=seq)
        return seq

    def on_payment(self, address, order_id, tx: SimplifiedTransaction):
        """handle a new payment from/to an address."""
        payment = Payment.from_blockchain(tx)
        log.info(f'got payment for order {order_id} for address {address}', payment=payment)
        statsd.inc_count('payment_observed',
                         payment.amount,
                         tags=['app_id:%s' % payment.app_id,
                               'address:%s' % address])

        self.enqueuer.enqueue_payment_callback(self.webhook, address, payment)
        self.remove(address, order_id)

    async def worker(self):
        """Poll blockchain and apply callback on watched address. run until stopped."""
        ledger_seq = await self.get_last_ledger_sequence()
        retry_count = 0
        while True:
            if retry_count == 0:  # Otherwise, stay with the old start_time
                start_t = time.time()
            try:
                addresses = self.watched_addresses.keys()
                log.debug(f'Watching for ledger {ledger_seq}, try: #{retry_count + 1}')

                try:
                    txs = await self.bc_manager.get_txs_per_ledger(ledger_seq)
                    retry_count = 0
                except ResourceNotFoundError:
                    log.debug(f'Ledger {ledger_seq} not found')
                    retry_count += 1
                    if retry_count >= RETRIES_BEFORE_EXCEPTION:
                        raise

                    log.debug(f'retrying in {WAIT_WHEN_NOT_FOUND} seconds')
                    await asyncio.sleep(WAIT_WHEN_NOT_FOUND)
                    continue

                if len(txs) != 0:
                    """
                    get_txs_per_ledger returns a list of responses from horizon.
                    Building the Transaction object, which includes decoding the envelops of potentially hundreds of txs 
                    is cpu intensive and can take a lot of time, (~80ms per 200 txs with 1 op each)
                    we shouln't block the event loop for that long, so run it in a separate process
                    """

                    txs = await self.loop.run_in_executor(self.executor, get_kin_payments, txs)
                    for tx in txs:
                        try:
                            _, _, order_id = tx.memo.split('-')
                        except (AttributeError, ValueError):
                            continue  # Not one of our orders

                        if (tx.source in addresses and
                                order_id in self.watched_addresses[tx.source]):
                            self.on_payment(tx.source, order_id, tx)

                        if (tx.operation.destination in addresses and
                                order_id in self.watched_addresses[tx.operation.destination]):
                            self.on_payment(tx.operation.destination, order_id, tx)
            except asyncio.CancelledError:
                raise  # Need this to be able to cancel the worker
            except Exception as e:
                statsd.increment('watcher_beat.failed', tags=['error:%s' % e])
                log.exception('failed watcher iteration', error=e)
            else:
                ledger_seq += 1

            statsd.timing('watcher_beat', time.time() - start_t)
            statsd.gauge('watcher_beat.ledger', ledger_seq)

            await asyncio.sleep(WAIT_WHEN_FOUND)


def get_kin_payments(txs: List[Dict]) -> List[SimplifiedTransaction]:
    result = []
    for tx in txs:
        try:
            simplified = SimplifiedTransaction(RawTransaction(tx))
        except CantSimplifyError:
            continue  # We dont care about these txs

        if simplified.operation.type == OperationTypes.PAYMENT:
            result.append(simplified)

        """
        It would be better to also created the 'Payment' object here, but schematics objects cant be pickled,
        so its not possible to transfer them between processes. I'll do it in "on_payment"
        """

    return result
