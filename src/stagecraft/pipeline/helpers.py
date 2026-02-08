import inspect
from typing import TYPE_CHECKING, Callable, Optional, TypeVar, Union

if TYPE_CHECKING:
    from .stages import ETLStage

_T = TypeVar("_T")
_R = TypeVar("_R")

SValuable = Union[_R, Callable[[], _R], Callable[["ETLStage"], _R]]


def resolve_svaluable(
    value: Optional[SValuable[_T]],
    stage: Optional["ETLStage"] = None,
) -> Optional[_T]:
    if value is None:
        return None
    if callable(value):
        sig = inspect.signature(value)
        if len(sig.parameters) == 0:
            return value()  # type: ignore[return-value]
        elif stage is None:
            raise ValueError("Cannot resolve SValuable because stage parameter is None.")
        else:
            return value(stage)  # type: ignore[return-value]
    return value
