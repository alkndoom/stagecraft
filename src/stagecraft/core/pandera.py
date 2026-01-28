from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
import pandera.pandas as pa

from .dataclass import AutoDataClass, autodataclass
from .types import PaDataFrame

ListLikeU = Union[List, pa.typing.Series, pa.typing.Index]
Hashable = Union[str, int, float, bytes]
DataFrame = Union[pa.typing.DataFrame, PaDataFrame[Any], pd.DataFrame]
Axes = Union[ListLikeU, pa.typing.Index, pd.Index]

_T = TypeVar("_T", bound="PaDataFrameModel")

pafield = pa.Field


class PaDataFrameModel(pa.DataFrameModel):

    @classmethod
    def DF(
        cls: Type[_T],
        data: Optional[
            Union[
                ListLikeU,
                DataFrame,
                Dict[Any, Any],
                Iterable[Union[ListLikeU, Tuple[Hashable, ListLikeU], Dict[Any, Any]]],
            ]
        ] = None,
        index: Axes | None = None,
        columns: Axes | None = None,
        copy: bool | None = None,
    ) -> PaDataFrame[_T]:
        return PaDataFrame[_T](data, index=index, columns=columns, copy=copy)


@autodataclass
class PaConfig(AutoDataClass):
    """Configuration class for Pandera DataFrame validation schemas.

    This immutable configuration class provides a comprehensive set of options for
    controlling Pandera's DataFrame validation behavior. It extends AutoDataClass and BaseConfig,
    designed to be used with StageCraft's DFVarSchema and Pandera's DataFrameModel or DataFrameSchema
    to define validation rules, type coercion, data transformation, and serialization
    settings.

    The class supports various validation modes including strict column checking,
    type coercion, invalid row handling, and multi-index validation. It also provides
    options for data format conversion both before and after validation.

    Attributes:
        add_missing_columns: If True, adds columns defined in the schema to the
            DataFrame if they are missing during validation. Default is False.
        coerce: If True, attempts to coerce all schema components to their specified
            data types during validation. Default is False.
        description: Optional textual description of the schema for documentation
            purposes. Default is None.
        drop_invalid_rows: If True, drops rows that fail validation instead of
            raising an error. Default is False.
        dtype: Optional dictionary mapping column names to their expected data types.
            Default is None.
        from_format: Optional string specifying the data format to convert from
            before validation (e.g., 'csv', 'json', 'parquet'). Default is None.
        from_format_kwargs: Optional dictionary of keyword arguments to pass to
            the reader function when converting from the specified format. Default is None.
        metadata: Optional dictionary for storing arbitrary key-value data at the
            schema level. Default is None.
        multiindex_coerce: If True, coerces all MultiIndex components to their
            specified types. Default is False.
        multiindex_name: Optional name for the MultiIndex. Default is None.
        multiindex_strict: If True or 'filter', validates that MultiIndex columns
            appear in the specified order. If 'filter', removes MultiIndex columns
            not in the schema. Default is False.
        multiindex_unique: If True, ensures the MultiIndex is unique along the
            list of columns. Default is False.
        name: Optional name identifier for the schema. Default is None.
        ordered: If True or 'filter', validates that columns appear in the order
            specified in the schema. If 'filter', reorders columns to match schema.
            Default is False.
        strict: If True, ensures all columns specified in the schema are present
            in the DataFrame. If 'filter', removes columns not specified in the
            schema. Default is False.
        title: Optional human-readable label for the schema, useful for
            documentation and error messages. Default is None.
        to_format: Optional string specifying the data format to serialize into
            after validation (e.g., 'csv', 'json', 'parquet'). Default is None.
        to_format_buffer: Optional buffer object to be provided when to_format
            is a custom callable. Default is None.
        to_format_kwargs: Optional dictionary of keyword arguments to pass to
            the writer function when converting to the specified format. Default is None.
        unique: If True, ensures all rows are unique. If a list of column names,
            ensures those column combinations are unique. Default is False.
        unique_column_names: If True, ensures all DataFrame column names are unique.
            Default is False.

    Example:
        >>> config = PaConfig(
        ...     coerce=True,
        ...     strict='filter',
        ...     drop_invalid_rows=True,
        ...     name='my_schema',
        ...     description='Schema for validating user data'
        ... )
        >>> class UserSchema(DFVarSchema):
        ...     class Config:
        ...         coerce = config.coerce
        ...         strict = config.strict

    Note:
        This class is frozen and uses slots for memory efficiency. All instances
        are immutable after creation.
    """

    add_missing_columns: bool = False
    coerce: bool = False
    description: Union[str, None] = None
    drop_invalid_rows: bool = False
    dtype: Union[dict, None] = None
    from_format: Union[str, None] = None
    from_format_kwargs: Union[dict, None] = None
    metadata: Union[dict, None] = None
    multiindex_coerce: bool = False
    multiindex_name: Union[str, None] = None
    multiindex_strict: Union[bool, str] = False
    multiindex_unique: bool = False
    name: Union[str, None] = None
    ordered: Union[bool, str] = False
    strict: Union[bool, str] = False
    title: Union[str, None] = None
    to_format: Union[str, None] = None
    to_format_buffer: Union[object, None] = None
    to_format_kwargs: Union[dict, None] = None
    unique: Union[bool, List[str]] = False
    unique_column_names: bool = False

    def get_pandera_config_dict(self) -> dict:
        return {
            "add_missing_columns": self.add_missing_columns,
            "coerce": self.coerce,
            "description": self.description,
            "drop_invalid_rows": self.drop_invalid_rows,
            "dtype": self.dtype,
            "from_format": self.from_format,
            "from_format_kwargs": self.from_format_kwargs,
            "metadata": self.metadata,
            "multiindex_coerce": self.multiindex_coerce,
            "multiindex_name": self.multiindex_name,
            "multiindex_strict": self.multiindex_strict,
            "multiindex_unique": self.multiindex_unique,
            "name": self.name,
            "ordered": self.ordered,
            "strict": self.strict,
            "title": self.title,
            "to_format": self.to_format,
            "to_format_buffer": self.to_format_buffer,
            "to_format_kwargs": self.to_format_kwargs,
            "unique": self.unique,
            "unique_column_names": self.unique_column_names,
        }


__all__ = [
    "pafield",
    "PaDataFrameModel",
    "PaConfig",
]
