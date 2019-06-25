from . import config
from .log import init as init_log

import asyncio
from sanic import Sanic
from sanic.request import Request
from aioredis import Redis

from .statsd import statsd
from .errors import AlreadyExistsError, PaymentNotFoundError, NoSuchServiceError
from .middleware import init_middlewares
from .models import Payment, WalletRequest, PaymentRequest, WhitelistRequest
from .queue import Enqueuer
from .utils import json_response
from .blockchain import BlockchainManager
from .watcher import PaymentWatcher

log = init_log()

app = Sanic(__name__)
init_middlewares(app, config)

# Just for the ide intellisense
app.enqueuer: Enqueuer
app.redis: Redis
app.blockchain_manager: BlockchainManager
app.watcher: PaymentWatcher


@app.route('/wallets', methods=['POST'])
async def create_wallet(request: Request):
    body = WalletRequest(request.json)

    app.enqueuer.enqueue_create_wallet(body)
    return json_response({}, 200)


@app.route('/wallets/<wallet_address>', methods=['GET'])
async def get_wallet(request, wallet_address):
    w = await app.blockchain_manager.get_wallet(wallet_address)
    return json_response(w.to_primitive(), 200)


@app.route('/payments', methods=['POST'])
async def pay(request):
    payment = PaymentRequest(request.json)

    try:
        await Payment.get(payment.id, app.redis)
        raise AlreadyExistsError('payment already exists')
    except PaymentNotFoundError:
        pass

    if payment.is_external:
        # If order is external pay from the ds wallet
        if payment.app_id not in config.APP_SEEDS:
            raise NoSuchServiceError('Did not find keypair for service: {}'.format(payment.app_id))

    app.enqueuer.enqueue_send_payment(payment)
    return json_response({}, 201)


@app.route('/watchers/<service_id>', methods=['POST'])
def watch(request, service_id):
    body = request.json
    for address in body['wallet_addresses']:
        app.watcher.watch(address, body['order_id'])

    return json_response({}, 200)


@app.route('/whitelist', methods=['POST'])
async def whitelist(request):
    whitelist_request = WhitelistRequest(request.json)
    whitelist_request.verify_transaction()
    # Transaction is verified, whitelist it and return to marketplace
    whitelisted_tx = whitelist_request.whitelist(app.blockchain_manager)
    return json_response({'tx': whitelisted_tx}, 200)


@app.route('/status', methods=['GET'])
async def status(request):
    log.info('status received')
    statsd.gauge('pending_tasks', len(asyncio.all_tasks()))
    return json_response({'app_name': config.APP_NAME,
                          'status': 'ok',
                          'start_time': config.build['start_time'],
                          'build': {'timestamp': config.build['timestamp'],
                                    'commit': config.build['commit']}}, 200)


@app.route('/config', methods=['GET'])
async def get_config(request):
    return json_response({'horizon_url': config.STELLAR_HORIZON_URL,
                          'network_passphrase': config.STELLAR_NETWORK}, 200)
