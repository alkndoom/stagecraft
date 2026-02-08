import functools
import logging
from typing import Any, Callable, Optional, TypeVar, overload

from .exceptions import AppException, CriticalException

logger = logging.getLogger(__name__)


F = TypeVar("F", bound=Callable[..., Any])


def exceptional(func):
    """Decorator to log exceptions and trace function calls with details."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt is detected.")
            raise
        except AppException:
            raise
        except CriticalException as e:
            logger.critical(AppException(e, func.__name__))
            exit()
        except Exception as e:
            raise AppException(e, func.__name__) from e

    return wrapper


@overload
def nullable(func: F) -> F: ...


@overload
def nullable(*, default: Any = None) -> Callable[[F], F]: ...


def nullable(
    func: Optional[Callable[..., Any]] = None,
    *,
    default: Any = None,
) -> Any:
    """Decorator to return default value on exception and log error."""

    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return f(*args, **kwargs)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # logger.warning(AppException(e, f.__name__))
                return default

        return wrapper

    if func is None:
        return decorator
    return decorator(func)
