import datetime

from cwm_worker_tests.retry import retry_exception_decorator


def call_raise_exception(f, msg):
    start_time = datetime.datetime.now()
    try:
        return f(msg), None, (datetime.datetime.now() - start_time).total_seconds()
    except Exception as e:
        return None, '{}:{}'.format(e.__class__, str(e)), (datetime.datetime.now() - start_time).total_seconds()


def test_retry_exception():

    @retry_exception_decorator(retries_seconds=[0.1, 0.2, 0.3])
    def raise_exception(msg):
        raise Exception(msg)

    res, error, seconds = call_raise_exception(raise_exception, 'Hello World!')
    assert res is None and error == "<class 'Exception'>:Hello World!" and 0.6 <= seconds <= 0.7

    @retry_exception_decorator(retries_seconds=[0.1, 0.2, 0.3], exceptions=[NotImplementedError])
    def raise_exception(msg):
        raise Exception(msg)

    res, error, seconds = call_raise_exception(raise_exception, 'foo')
    assert res is None and error == "<class 'Exception'>:foo" and seconds < 0.1

    @retry_exception_decorator(retries_seconds=[0.1], exceptions=[NotImplementedError])
    def raise_exception(msg):
        raise NotImplementedError(msg)

    res, error, seconds = call_raise_exception(raise_exception, 'foo')
    assert res is None and error == "<class 'NotImplementedError'>:foo" and 0.1 <= seconds <= 0.2

    @retry_exception_decorator(retries_seconds=[0.1], exceptions=[NotImplementedError])
    def raise_exception(msg):
        return msg

    res, error, seconds = call_raise_exception(raise_exception, 'foo')
    assert res == 'foo' and error is None and seconds < 0.1
