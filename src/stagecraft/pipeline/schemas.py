import copy
import hashlib
from dataclasses import field
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import numpy as np
import pandera.pandas as pa
import pyarrow as pa_arrow
from pandera.api.dataframe.model_components import FieldInfo
from pandera.api.pandas.model_config import BaseConfig
from pandera.typing import Index, Series

from ..core.dataclass import AutoDataClass, autodataclass
from ..core.pandera import PaConfig, PaDataFrame, PaDataFrameModel
from ..core.types import NDArrayGen

Indexable = Union[NDArrayGen, Sequence[Any]]

_SCHEMA = TypeVar("_SCHEMA", bound="DFVarSchema")


_SCHEMA_CACHE: Dict[str, Tuple[Type[PaDataFrameModel], Dict[str, List[str]]]] = {}


def _schema_signature(fields: List[Tuple]) -> str:
    raw = repr(fields).encode()
    return hashlib.sha256(raw).hexdigest()


def _collect_annotations(cls: Type) -> Dict[str, object]:
    merged: Dict[str, object] = {}
    for base in reversed(cls.__mro__):
        if issubclass(base, DFVarSchema):
            merged.update(get_type_hints(base, include_extras=True))
    merged = {
        k: v for k, v in merged.items() if (not k.startswith("_") and (k not in ("M", "dtypes")))
    }
    return merged


def _parse_type(tp: object) -> Tuple[Type, bool, Optional[FieldInfo]]:
    field_constraints: Optional[FieldInfo] = None

    if get_origin(tp) is Annotated:
        base, *extras = get_args(tp)
        tp = base
        for extra in extras:
            if isinstance(extra, FieldInfo):
                field_constraints = extra

    nullable = False
    if get_origin(tp) is Union and type(None) in get_args(tp):
        nullable = True
        tp = next(t for t in get_args(tp) if t is not type(None))

    return tp, nullable, field_constraints  # type: ignore


def _build_pandera_field_info(
    nullable: bool,
    field_constraints: Optional[FieldInfo],
) -> Optional[FieldInfo]:
    if field_constraints:
        fc = copy.copy(field_constraints)
        if nullable:
            fc.nullable = True
        return fc
    elif nullable:
        return pa.Field(nullable=True)
    return None


def _standardize_dtype(tp: Type, nullable: bool) -> str:
    """Map type annotations to standardized dtypes for Pandas. tp can be int, float, str, bool, etc.
    or numpy dtypes like numpy.int64, numpy.float64, etc.
    """

    if tp in (int, np.integer):
        return "Int64" if nullable else "int64"
    elif tp in (float, np.floating):
        return "Float64" if nullable else "float64"
    elif tp in (bool, np.bool_):
        return "Boolean" if nullable else "boolean"
    elif tp in (str, np.str_):
        return "String" if nullable else "string"
    else:
        return "Other"


def _signature_entry(
    name: str,
    tp: Type,
    nullable: bool,
    config: PaConfig,
    field_constraints: Optional[FieldInfo],
) -> Tuple:
    checks = []
    if field_constraints:
        for check in field_constraints.checks:
            checks.append((check.name, tuple(check.statistics)))
    return (name, tp, nullable, config, tuple(checks))


def _model_to_arrow(model: Type[PaDataFrameModel]) -> pa_arrow.Schema:
    schema = model.to_schema()
    fields = []
    for name, col in schema.columns.items():
        arrow_type = pa_arrow.from_numpy_dtype(col.dtype.type)
        fields.append(pa_arrow.field(name, arrow_type, nullable=col.nullable))
    return pa_arrow.schema(fields)


@autodataclass
class DFVarSchema(AutoDataClass):

    M: ClassVar[Type[PaDataFrameModel]] = field(init=False, repr=False)
    dtypes: ClassVar[Dict[str, Any]] = field(init=False, repr=False)
    __arrow_schema: ClassVar[pa_arrow.Schema] = field(init=False, repr=False)

    def __init_subclass__(
        cls,
        *,
        config: Optional[PaConfig] = None,
        index_cols: Optional[List[str]] = None,
        **kwargs,
    ):
        config = config or PaConfig(strict=True, coerce=True)
        index_cols = index_cols or []

        merged_hints = _collect_annotations(cls)

        missing_index_cols = set(index_cols).difference(merged_hints.keys())
        if missing_index_cols:
            raise ValueError(
                f"Missing index columns for schema '{cls.__name__}': {missing_index_cols}"
            )

        pandera_annotations: Dict[str, object] = {}
        pandera_fields: Dict[str, FieldInfo] = {}
        signature_fields: List[Tuple] = []
        dtypes: Dict[str, List[str]] = {}

        for name, raw_tp in merged_hints.items():
            is_index = name in index_cols
            tp, nullable, field_constraints = _parse_type(raw_tp)

            dtype = _standardize_dtype(tp, nullable)
            if dtype in dtypes:
                dtypes[dtype].append(name)
            else:
                dtypes[dtype] = [name]

            pandera_annotations[name] = Index[tp] if is_index else Series[tp]

            field_info = _build_pandera_field_info(nullable, field_constraints)
            if field_info is not None:
                pandera_fields[name] = field_info

            signature_fields.append(_signature_entry(name, tp, nullable, config, field_constraints))

        sig = _schema_signature(signature_fields)
        if sig in _SCHEMA_CACHE:
            cls.M, cls.dtypes = _SCHEMA_CACHE[sig]
            return

        cls.M = type(
            f"{cls.__name__}DataFrameModel",
            (PaDataFrameModel,),
            {
                "__annotations__": pandera_annotations,
                **pandera_fields,
                "Config": type("Config", (BaseConfig,), {**config.get_pandera_config_dict()}),
            },
        )
        cls.dtypes = dtypes
        _SCHEMA_CACHE[sig] = (cls.M, cls.dtypes)

    @classmethod
    def to_arrow(cls: Type[_SCHEMA]) -> pa_arrow.Schema:
        if not hasattr(cls, "__arrow_schema"):
            cls.__arrow_schema = _model_to_arrow(cls.M)
        return cls.__arrow_schema

    @staticmethod
    def to_vect_dict(instance: Union[_SCHEMA, List[_SCHEMA]]) -> Dict[str, NDArrayGen]:
        if not isinstance(instance, list):
            instance = [instance]
        if not instance:
            return {}

        cls = type(instance[0])
        slot_names = cls.__slots__  # type: ignore
        result = {}
        n = len(instance)

        for name in slot_names:
            dtype = cls.dtypes.get(name, None)

            if dtype and dtype[0] in ("int64", "Int64"):
                col = np.empty(n, dtype=np.int64)
                col = np.array([getattr(inst, name) for inst in instance], dtype=np.int64)
            elif dtype and dtype[0] in ("float64", "Float64"):
                col = np.empty(n, dtype=np.float64)
                col = np.array([getattr(inst, name) for inst in instance], dtype=np.float64)
            elif dtype and dtype[0] in ("boolean", "Boolean"):
                col = np.empty(n, dtype=np.bool_)
                col = np.array([getattr(inst, name) for inst in instance], dtype=np.bool_)
            elif dtype and dtype[0] in ("string", "String"):
                col = np.empty(n, dtype=np.str_)
                col = np.array([getattr(inst, name) for inst in instance], dtype=np.str_)
            else:
                col = np.empty(n, dtype=object)
                col = np.array([getattr(inst, name) for inst in instance], dtype=object)

            result[name] = col
        return result

    @classmethod
    def from_dict(cls: Type[_SCHEMA], data: Mapping[str, Indexable]) -> List[_SCHEMA]:
        if not data:
            return []

        cols = tuple(data.keys())
        n = len(data[cols[0]])

        return [cls(**{col: data[col][i] for col in cols}) for i in range(n)]

    @staticmethod
    def to_dataframe(instance: Union[_SCHEMA, List[_SCHEMA]]) -> PaDataFrame[_SCHEMA]:
        return PaDataFrame[_SCHEMA](DFVarSchema.to_vect_dict(instance))

    @classmethod
    def from_dataframe(cls: Type[_SCHEMA], df: PaDataFrame[_SCHEMA]) -> List[_SCHEMA]:
        data = {col: df[col].to_numpy() for col in df.columns}
        return cls.from_dict(data)
