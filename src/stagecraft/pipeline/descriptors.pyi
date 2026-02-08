from typing import Any, Optional, Type, Union, overload

from ..core.pandera import PaDataFrame
from .helpers import SValuable
from .variables import _SCHEMA, _T, DFVar, SVar

# -------------------Consume overloads ------------------- #
@overload
def sconsume(
    var: Union[Type[_T], SVar[_T]],
    /,
    *,
    value: Optional[SValuable[_T]] = ...,
    force_overwrite: bool = False,
) -> _T: ...
@overload
def sconsume(
    var: DFVar[_SCHEMA],
    /,
    *,
    value: Optional[SValuable[PaDataFrame[_SCHEMA]]] = ...,
    force_overwrite: bool = False,
) -> PaDataFrame[_SCHEMA]: ...
@overload
def sconsume(var: None = ...) -> Any: ...

# -------------------Transform overloads ------------------- #
@overload
def stransform(
    var: Union[Type[_T], SVar[_T]],
    /,
    *,
    value: Optional[SValuable[_T]] = ...,
    force_overwrite: bool = False,
) -> _T: ...
@overload
def stransform(
    var: DFVar[_SCHEMA],
    /,
    *,
    value: Optional[SValuable[PaDataFrame[_SCHEMA]]] = ...,
    force_overwrite: bool = False,
) -> PaDataFrame[_SCHEMA]: ...
@overload
def stransform(var: None = ...) -> Any: ...

# -------------------Produce overloads ------------------- #
@overload
def sproduce(var: Union[Type[_T], SVar[_T]], /) -> _T: ...
@overload
def sproduce(var: DFVar[_SCHEMA], /) -> PaDataFrame[_SCHEMA]: ...
@overload
def sproduce(var: None = ...) -> Any: ...
