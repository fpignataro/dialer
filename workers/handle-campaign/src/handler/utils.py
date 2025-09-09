from functools import lru_cache, wraps
from datetime import datetime, timedelta, UTC


def timed_lru_cache(seconds: int, maxsize: int = 128):
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.now(UTC) + func.lifetime

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.now(UTC) >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.now(UTC) + func.lifetime

            return func(*args, **kwargs)

        wrapped_func.cache_clear = func.cache_clear
        wrapped_func.cache_info = func.cache_info
        wrapped_func.cache_parameters = func.cache_parameters

        return wrapped_func

    return wrapper_cache
