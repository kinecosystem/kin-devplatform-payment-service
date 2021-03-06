class BaseError(Exception):
    http_code = 400
    code = 4000

    def __init__(self, message):
        self.message = message

    def to_dict(self):
        return {'code': self.code, 'error': self.message}


class AlreadyExistsError(BaseError):
    http_code = 409  # conflict
    code = 4091


class PaymentNotFoundError(BaseError):
    http_code = 404
    code = 4041


class WalletNotFoundError(BaseError):
    http_code = 404
    code = 4042


class NoSuchServiceError(BaseError):
    http_code = 404
    code = 4043


class OrderNotFoundError(BaseError):
    http_code = 404
    code = 4044


class TransactionMismatch(BaseError):
    http_code = 401
    code = 4015


class ParseError(ValueError):
    pass
