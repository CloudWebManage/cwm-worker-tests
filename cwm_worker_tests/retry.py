import time
import traceback


def retry_exception_decorator(retries_seconds=None, exceptions=None):
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
