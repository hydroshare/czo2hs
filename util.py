import logging
import time


def retry_func(fun, args=None, kwargs=None, max_tries=4, interval_sec=5, increase_interval=True):
    """
    TODO docstring
    :param fun:
    :param args:
    :param kwargs:
    :param max_tries:
    :param interval_sec:
    :param increase_interval:
    :return:
    """
    pass_on_args = args if args else []
    pass_on_kwargs = kwargs if kwargs else {}

    for i in range(max_tries):
        try:
            func_result = fun(*pass_on_args, **pass_on_kwargs)
            return func_result
        except Exception as ex:
            if i == max_tries - 1:
                raise ex
            else:
                logging.warning("Failed to call {}, retrying {}/{}".format(str(fun), str(i+1), str(max_tries-1)))

            if increase_interval:
                time.sleep(interval_sec*(i+1))
            else:
                time.sleep(interval_sec)
            continue


