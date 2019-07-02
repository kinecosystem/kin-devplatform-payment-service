from payment.app import app, config

app.run(access_log=False, host='0.0.0.0', port=config.APP_PORT)
