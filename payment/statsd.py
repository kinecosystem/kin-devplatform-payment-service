import types

from datadog import DogStatsd
from .config import STATSD_PORT, STATSD_HOST

statsd = DogStatsd(STATSD_HOST, STATSD_PORT, namespace='payment')


def inc_count(self, metric, value, tags):
    """both increment the metric by the given value and set a counter on it."""
    self.increment(metric, value, tags=tags)
    self.increment('%s.count' % metric, tags=tags)


statsd.inc_count = types.MethodType(inc_count, statsd)


def task_error_handler(loop, context):
    # Just report to statsd and call the default error handler
    try:
        func_name = context['future']._coro.cr_code.co_name
        exc = context['exception']
        statsd.increment('worker_error', tags=['job:%s' % func_name, 'error_type:%s' % exc])
    except:
        pass

    loop.default_exception_handler(context)
