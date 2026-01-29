from dataclasses import field
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

import pyarrow as pa_arrow

from ..core.dataclass import AutoDataClass, autodataclass
from ..core.pandera import PaConfig, PaDataFrame, PaDataFrameModel
from ..core.types import NDArrayGen

_SCHEMA = TypeVar("_SCHEMA", bound="DFVarSchema")

@autodataclass
class DFVarSchema(AutoDataClass):

    M: ClassVar[Type[PaDataFrameModel]] = field(init=False, repr=False)
    A: ClassVar[pa_arrow.Schema] = field(init=False, repr=False)
    dtypes: ClassVar[Dict[str, Any]] = field(init=False, repr=False)

    def __init_subclass__(
        cls,
        *,
        config: Optional[PaConfig] = None,
        index_cols: Optional[List[str]] = None,
        **kwargs,
    ) -> None: ...
    @staticmethod
    def to_dict(instances: List[_SCHEMA]) -> Dict[str, NDArrayGen]: ...
    @staticmethod
    def to_dataframe(instances: List[_SCHEMA]) -> PaDataFrame[_SCHEMA]: ...
