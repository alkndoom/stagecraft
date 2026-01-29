from __future__ import annotations

from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union

import pandas as pd
import pandera
import pandera.pandas as pa

from .dataclass import AutoDataClass, autodataclass

ListLikeU = Union[List, pa.typing.Series, pa.typing.Index]
Hashable = Union[str, int, float, bytes]
DataFrame = Union[pa.typing.DataFrame, pandera.typing.DataFrame[Any], pd.DataFrame]
Axes = Union[ListLikeU, pa.typing.Index, pd.Index]

_T = TypeVar("_T")

pafield = pa.Field


class PaDataFrame(Generic[_T], pandera.typing.DataFrame[_T]):

    def iter_chunks(self, chunk_size: int) -> Iterable[PaDataFrame[_T]]:
        """Iterate over the DataFrame in chunks for memory-efficient processing.

        This generator yields consecutive chunks of the DataFrame, allowing
        processing of large DataFrames without loading all data into memory
        at once. Useful for operations that can be applied row-wise or in batches.

        Args:
            chunk_size: Number of rows per chunk. Must be positive.

        Yields:
            PaDataFrame chunks of size chunk_size (last chunk may be smaller).

        Raises:
            ValueError: If chunk_size is not positive.

        Example:
            >>> var = PaDataFrame[Model]({'id': range(10000), 'value': range(10000)})
            >>> for chunk in var.iter_chunks(1000):
            ...     # Process each 1000-row chunk
            ...     print(f"Processing {len(chunk)} rows")
            ...     result = expensive_operation(chunk)
            Processing 1000 rows
            Processing 1000 rows
            ...
            Processing 1000 rows

        Note:
            If the DataFrame value is None, this method returns immediately
            without yielding any chunks.
        """
        if chunk_size <= 0:
            raise ValueError(
                f"chunk_size must be positive, got {chunk_size}. "
                f"Use a positive integer for the number of rows per chunk."
            )

        total_rows = len(self)
        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            # .iloc[] returns type Series[Any] because of an error in pandas stubs
            yield self.iloc[start_idx:end_idx]  # type: ignore

    def process_in_chunks(
        self,
        chunk_size: int,
        process_fn: Callable[[PaDataFrame[_T]], PaDataFrame[_T]],
    ) -> PaDataFrame[_T]:
        """Process the DataFrame in chunks and concatenate the results.

        This method provides memory-efficient processing of large DataFrames by:
        1. Splitting the DataFrame into chunks of specified size
        2. Applying a processing function to each chunk
        3. Concatenating all processed chunks into a single result DataFrame

        This is particularly useful for operations that would consume too much
        memory if applied to the entire DataFrame at once, such as:
        - Complex transformations
        - Filtering operations
        - Feature engineering
        - API calls or database lookups per row

        Args:
            chunk_size: Number of rows per chunk. Must be positive.
            process_fn: Function that takes a PaDataFrame chunk and returns a
                       processed PaDataFrame chunk. The function should maintain
                       the schema type.

        Returns:
            A new PaDataFrame containing all processed chunks concatenated together.

        Raises:
            ValueError: If chunk_size is not positive or process_fn is None.
            RuntimeError: If processing any chunk fails. The error message includes
                         the chunk index and row range for debugging.

        Example:
            >>> var = PaDataFrame[Model]({'id': range(10000), 'value': range(10000)})
            >>>
            >>> # Filter in chunks
            >>> result = var.process_in_chunks(
            ...     chunk_size=1000,
            ...     process_fn=lambda chunk: chunk[chunk['value'] > 5000]
            ... )
            >>> print(len(result))
            4999
            >>>
            >>> # Transform in chunks
            >>> result = var.process_in_chunks(
            ...     chunk_size=1000,
            ...     process_fn=lambda chunk: chunk.assign(
            ...         value_squared=chunk['value'] ** 2
            ...     )
            ... )

        Note:
            The concatenation uses ignore_index=True, so the resulting DataFrame
            will have a new sequential index starting from 0.
        """

        if chunk_size <= 0:
            raise ValueError(
                f"chunk_size must be positive, got {chunk_size}. "
                f"Use a positive integer for the number of rows per chunk."
            )

        if process_fn is None:
            raise ValueError("process_fn cannot be None. Provide a function to process each chunk.")

        try:
            processed_chunks = []
            for i, chunk in enumerate(self.iter_chunks(chunk_size)):
                try:
                    processed_chunk = process_fn(chunk)
                    processed_chunks.append(processed_chunk)
                except Exception as e:
                    raise RuntimeError(
                        f"Error processing chunk {i} (rows {i*chunk_size} to {(i+1)*chunk_size}) "
                        f"of PaDataFrame '{self.__class__.__name__}'.\nOriginal error: {str(e)}"
                    ) from e

            result = pd.concat(processed_chunks, ignore_index=True)
            return PaDataFrame[_T](result)
        except Exception as e:
            if isinstance(e, RuntimeError) and "Error processing chunk" in str(e):
                raise
            raise RuntimeError(
                f"Failed to process PaDataFrame '{self.__class__.__name__}' in chunks.\n"
                f"Original error: {str(e)}"
            ) from e


class PaDataFrameModel(pa.DataFrameModel, Generic[_T]):

    @classmethod
    def DF(
        cls,
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
    "PaDataFrame",
    "PaDataFrameModel",
    "PaConfig",
]
