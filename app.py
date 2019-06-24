from payment.app import app, config

app.run(access_log=False, port=config.APP_PORT)
