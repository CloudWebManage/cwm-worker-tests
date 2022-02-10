import time
import traceback


def retry_exception_decorator(retries_seconds=None, exceptions=None):
    """Decorator to apply retry to function
    example usage:

    @retry_exception_decorator([1, 2, 3], [ThisException, ThatException])
    def my_function():
      do something..

    the function can be called normally, if it raises ThisException or ThatException it
    will retry, sleeping the given number of seconds before each retry
    so that, first retry it will sleep 1 seconds
    second, will sleep 2 seconds
    third, will sleep 3 seconds
    finally, it it still raises an exception it will raise that exception
    """
    if retries_seconds is None:
        retries_seconds = [1, 2, 5, 10, 1, 10, 1, 20, 1]
    if exceptions is None:
        exceptions = [Exception]

    def _decorator(f):

        def _retry_exception(*args, **kwargs):
            for retry_num, retry_seconds in enumerate(retries_seconds + [None]):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if all([not isinstance(e, expected_exception) for expected_exception in exceptions]):
                        raise
                    elif retry_seconds is None:
                        raise
                    else:
                        traceback.print_exc()
                        print("Attempt {}/{} failed, retrying after {} seconds".format(retry_num + 1, len(retries_seconds) + 1, retry_seconds))
                        time.sleep(retry_seconds)

        return _retry_exception

    return _decorator


def retry_exception_dynamic(callback, calls_config, callback_args=None, callback_kwargs=None, exceptions=None):
    """
    dynamic invocation of retry callback, example usage:

    retry_exception_dynamic(
        my_function,
        [
            {'extra_args': ['arg3', 'arg4'], 'extra_kwargs': {'foo': 'bar'}},
            {'sleep_seconds': 5},
            {'sleep_seconds': 10, 'extra_args': ['baz', 'bax']}
        ],
        callback_args=['arg1', 'arg2'],
        callback_kwargs={'hi': 'bye'},
        exceptions=[MyException]
    )

    First attempt, the function will be called like so: my_function('arg1', 'arg2', 'arg3', 'arg4', hi='bye', foo='bar')
    if it raises MyException, second attempt will sleep 5 seconds, then call: my_function('arg1', 'arg2', hi='bye')
    if still raises MyException, third attempt will sleep 10 seconds and call my_function('arg1', 'arg2', hi='bye', baz='bax')
    If third attempt still raises MyException it will be raised
    """
    calls_config[-1]['__last'] = True
    if callback_args is None:
        callback_args = []
    if callback_kwargs is None:
        callback_kwargs = {}
    if exceptions is None:
        exceptions = [Exception]
    for call_num, call_config in enumerate(calls_config):
        if call_config.get('sleep_seconds'):
            print("Waiting {} seconds..".format(call_config['sleep_seconds']))
            time.sleep(call_config['sleep_seconds'])
        try:
            return callback(
                *[*callback_args, *call_config.get('extra_args', [])],
                **{**callback_kwargs, **call_config.get('extra_kwargs', {})}
            )
        except Exception as e:
            if all([not isinstance(e, expected_exception) for expected_exception in exceptions]):
                raise
            elif call_config.get('__last'):
                raise
            else:
                traceback.print_exc()
                print("Attempt {}/{} failed".format(call_num + 1, len(calls_config)))
