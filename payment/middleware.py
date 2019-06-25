import asyncio

import kin
from kin.utils import create_channels
import aioredis
from aioredis_lock import RedisLock

from .errors import BaseError
from .log import get as get_log
from .statsd import statsd, task_error_handler
from .utils import json_response
from .blockchain import BlockchainManager
from .queue import Enqueuer
from .watcher import PaymentWatcher

log = get_log()


def init_middlewares(app, config):
    @app.exception(Exception)
    def handle_errors(request, e):
        if not isinstance(e, BaseError):
            e = BaseError(str(e))
        log.exception('uncaught error', error=e, payload=e.to_dict(), message=e.message)
        statsd.increment('server_error',
                         tags=['path:%s' % request.path, 'error:%s' % e.message, 'method:%s' % request.method])
        return json_response(e.to_dict(), e.http_code)

    @app.listener('before_server_start')
    async def server_setup(app, loop):
        """Things to do before we start accepting requests"""

        # Set exception handler for the loop that reports to dd
        loop.set_exception_handler(task_error_handler)

        # Get redis connection
        app.redis = await aioredis.create_redis(config.APP_REDIS)

        # Setup Kin related stuff

        async def add_to_blockchain_manager(ds, seeds):
            async with RedisLock(app.redis, '_lock_ds_channels:{}'.format(ds),  # Lock to prevent bad_seq conflicts with other instances
                                 timeout=60, wait_timeout=30):
                channels = await create_channels(seeds.our, kin_env, config.MAX_CHANNELS, 0, config.CHANNEL_SALT)
                app.blockchain_manager.add_account(seeds.our, channels, ds)

                channels = await create_channels(seeds.joined, kin_env, config.MAX_CHANNELS, 0, config.CHANNEL_SALT)
                app.blockchain_manager.add_account(seeds.joined, channels, ds)

                log.debug(f'Initialized ds {ds} with {config.MAX_CHANNELS} per seed')

        # Setup kin client
        kin_env = kin.Environment('CUSTOM', config.STELLAR_HORIZON_URL, config.STELLAR_NETWORK)
        kin_client = kin.KinClient(kin_env)
        app.blockchain_manager = BlockchainManager(kin_client, await kin_client.get_minimum_fee())
        app.enqueuer = Enqueuer(app.blockchain_manager, app.redis)

        tasks = []
        for ds, seeds in config.APP_SEEDS.items():
            tasks.append(asyncio.create_task(add_to_blockchain_manager(ds, seeds)))

        await asyncio.gather(*tasks)

        # Start the payment watcher

        app.watcher = PaymentWatcher(app.enqueuer, app.blockchain_manager, config.WEBHOOK, app.redis)
        app.watcher.start()

    @app.listener('after_server_stop')
    async def server_teardown(app, loop):
        """Things to do after we stopped accepting requests"""

        # Wait for all pending tasks to finish
        tasks = asyncio.all_tasks(loop)
        log.info(f'Finishing {len(tasks)} pending tasks, sleeping for 30 seconds')
        await asyncio.sleep(30)

        # Stop the payment watcher
        app.watcher.stop()

        # Close remaining clients
        app.redis.close()
        await app.blockchain_manager.client.close()
