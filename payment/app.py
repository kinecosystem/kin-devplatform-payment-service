from . import config
from .log import init as init_log

log = init_log()
from flask import Flask, request, jsonify
from .transaction_flow import TransactionFlow
from .errors import AlreadyExistsError, PaymentNotFoundError, NoSuchServiceError
from .middleware import handle_errors
from .models import Payment, WalletRequest, PaymentRequest, Watcher, Service, WhitelistRequest
from .queue import enqueue_create_wallet, enqueue_send_payment
from .blockchain import Blockchain


app = Flask(__name__)


@app.route('/wallets', methods=['POST'])
@handle_errors
def create_wallet():
    body = WalletRequest(request.get_json())

    # wallet creation is idempotent - no locking needed
    enqueue_create_wallet(body)

    return jsonify(), 202


@app.route('/wallets/<wallet_address>', methods=['GET'])
@handle_errors
def get_wallet(wallet_address):
    w = Blockchain.get_wallet(wallet_address)
    return jsonify(w.to_primitive())


@app.route('/wallets/<wallet_address>/payments', methods=['GET'])
@handle_errors
def get_wallet_payments(wallet_address):
    payments = []
    flow = TransactionFlow(cursor=0)
    for tx in flow.get_address_transactions(wallet_address):
        payment = Blockchain.try_parse_payment(tx)
        if payment:
            payments.append(payment.to_primitive())

    return jsonify({'payments': payments})


@app.route('/payments/<payment_id>', methods=['GET'])
@handle_errors
def get_payment(payment_id):
    payment = Payment.get(payment_id)
    return jsonify(payment.to_primitive())


@app.route('/payments', methods=['POST'])
@handle_errors
def pay():
    payment = PaymentRequest(request.get_json())

    try:
        Payment.get(payment.id)
        raise AlreadyExistsError('payment already exists')
    except PaymentNotFoundError:
        pass

    if payment.is_external:
        # If order is external pay from the ds wallet
        if payment.app_id not in config.APP_SEEDS:
            raise NoSuchServiceError('Did not find keypair for service: {}'.format(payment.app_id))

    enqueue_send_payment(payment)
    return jsonify(), 201


@app.route('/watchers/<service_id>', methods=['POST', 'DELETE'])
@handle_errors
def watch(service_id):
    body = request.get_json()
    if request.method == 'DELETE':
        if Service.get(service_id) is not None:
            Watcher.remove(service_id, body['wallet_address'], body['order_id'])
            log.info('removed order {} from address {}'.format(body['order_id'], body['wallet_address']))
        else:
            raise NoSuchServiceError('There is no watcher for service: {}'.format(service_id))

    else:  # POST
        if Service.get(service_id) is None:
            # Add the service if it not in the database
            Service.new(service_id, body['callback'])
        for address in body['wallet_addresses']:
            Watcher.add(service_id, address, body['order_id'])
            log.info("Added order: {} to watcher for: {}".format(body['order_id'],address))

    return jsonify(), 200


@app.route('/whitelist', methods=['POST'])
def whitelist():
    print(request.get_json())
    print(type(request.get_json()))
    whitelist_request = WhitelistRequest(request.get_json())
    whitelist_request.verify_transaction()
    # Transaction is verified, whitelist it and return to marketplace
    whitelisted_tx = whitelist_request.whitelist()
    return jsonify({'tx': whitelisted_tx}), 200


@app.route('/status', methods=['GET'])
def status():
    body = request.get_json()
    log.info('status received', body=body)
    return jsonify({'app_name': config.APP_NAME,
                    'status': 'ok',
                    'start_time': config.build['start_time'],
                    'build': {'timestamp': config.build['timestamp'],
                              'commit': config.build['commit']}})


@app.route('/config', methods=['GET'])
def get_config():
    return jsonify({'horizon_url': config.STELLAR_HORIZON_URL,
                    'network_passphrase': (config.STELLAR_NETWORK),
                    })
