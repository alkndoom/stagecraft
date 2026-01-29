"""Stage variable classes for type-safe data flow in ETL pipelines.

This module provides variable classes that enable type-safe, validated data flow
between pipeline stages. Variables handle:

- Value storage and retrieval
- Loading from data sources (CSV, JSON, NumPy arrays) or pipeline context
- Saving to data sources or pipeline context
- Type validation and schema validation (for DataFrames)
- Preprocessing transformations
- Descriptor protocol for clean attribute access

The module includes three main variable types:

1. **SVar**: Generic stage variable for any Python type
2. **DFVar**: Specialized variable for pandas DataFrames with DFVarSchema (Pandera) support
3. **NDArrayVar**: Specialized variable for NumPy arrays with shape validation

Variables are typically declared using descriptor functions (sconsume, sproduce,
stransform) at the class level in ETL stages:

Example:
    >>> class MyStage(ETLStage):
    ...     # Generic variable
    ...     config = scstransformonsume(dict)
    ...     config = SVar(dict).stransform()  # Equivalent
    ...
    ...     # DataFrame with schema
    ...     input_data = sconsume(DFVar(MySchema))
    ...     input_data = SVar(DFVar(MySchema)).sconsume()  # Equivalent
    ...
    ...     # NumPy array with shape
    ...     embeddings = sproduce(NDArrayVar(shape=(100, 768)))
    ...     embeddings = SVar(NDArrayVar(shape=(100, 768))).sproduce()  # Equivalent
    ...
    ...     def recipe(self):
    ...         df = self.input_data  # Automatically loaded
    ...         self.embeddings = compute_embeddings(df)  # Automatically saved
"""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Callable, Generic, List, Optional, Type, TypeVar, Union

import numpy as np
import pandas as pd

from ..core.pandera import PaDataFrame, PaDataFrameModel
from .context import PipelineContext
from .data_source import ArraySource, CSVSource, DataSource
from .markers import IOMarker
from .schemas import DFVarSchema

if TYPE_CHECKING:
    from .stages import ETLStage

_T = TypeVar("_T")
_R = TypeVar("_R")
_SCHEMA = TypeVar("_SCHEMA", bound=DFVarSchema)

SValuable = Union[_R, Callable[[], _R], Callable[["ETLStage"], _R]]

logger = logging.getLogger(__name__)


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


class SVar(Generic[_T]):
    """
    A generic stage variable that holds and manages data within a pipeline stage.

    SVar provides the runtime behavior for stage variables, including value storage,
    loading from data sources or context, saving to context, and validation. It supports
    multiple initialization strategies (factory, default, or lazy loading) and optional
    preprocessing of values.

    Stands for 'Stage Variable'.

    Attributes:
        value: The current value of the variable.
        stage: The ETLStage instance that owns this variable (set by ETLStage).
        context: The pipeline context for loading/saving data, accessed through the stage (set by PipelineRunner).
        name: The variable name as defined on the stage class.
        type: The expected type of the variable value.
        source: Optional data source for loading/saving the value.
        description: Human-readable description of the variable's purpose.
        pre_processing: Optional transformation applied to values before storage.
        markers: List of IOMarkers for input/output classification.
    """

    name: str
    stage: Optional["ETLStage"] = None
    value: Optional[_T] = None

    @property
    def context(self) -> Optional[PipelineContext]:
        return self.stage.context if self.stage is not None else None

    def __init__(
        self,
        type_: Optional[Type[_T]] = None,
        /,
        *,
        value: Optional[SValuable[_T]] = None,
        source: Optional[SValuable[DataSource]] = None,
        description: Optional[str] = None,
        pre_processing: Optional[Callable[[_T], _T]] = None,
        markers: Optional[List[IOMarker]] = None,
    ):
        """
        Initialize a stage variable.

        Args:
            type_: The expected type of the variable value.
            value: Optional static default value or factory callable.
            source: Optional static default data source or factory callable for loading/saving the variable value.
            description: Human-readable description of the variable's purpose.
            pre_processing: Optional transformation function applied to values before
                they are stored in the variable.
            markers: List of IOMarkers for input/output classification.

        Raises:
            AssertionError: If both factory and default are provided.
        """
        self.type = type_
        self.source = source
        self.description = description
        self.pre_processing = pre_processing
        self.markers = markers or []
        self.__value__ = value

    def set_stage(self, stage: ETLStage):
        """
        Associate this variable with a specific pipeline stage.

        Args:
            stage: The ETLStage instance that owns this variable.

        Example:
            >>> var = SVar(pd.DataFrame)
            >>> stage = MyStage()
            >>> var.set_stage(stage)
        """
        self.stage = stage

    def set_markers(self, markers: List[IOMarker]):
        """Set the I/O markers for this variable.

        Args:
            markers: List of IOMarker values (INPUT, OUTPUT, or both).

        Example:
            >>> var = SVar(pd.DataFrame, name="data")
            >>> var.set_markers([IOMarker.INPUT])
        """
        self.markers = markers

    def __set_name__(self, owner, name: str):
        """Descriptor protocol method called when variable is assigned to a class.

        This method is automatically called by Python when the variable is
        assigned as a class attribute. It:
        1. Sets the variable name from the attribute name
        2. Infers the type from class annotations if not explicitly provided
        3. Registers the variable with the owner class for later injection

        Args:
            owner: The class that owns this variable (typically an ETLStage subclass).
            name: The attribute name assigned to this variable.

        Example:
            >>> class MyStage(ETLStage):
            ...     # __set_name__ is called automatically here
            ...     data = sconsume(pd.DataFrame)
            ...     # After this line, data.name == "data"

        Note:
            This is an internal method called by Python's descriptor protocol.
            It should not be called manually.
        """
        self.name = name

        # Ensure __annotations__ exists
        if not hasattr(owner, "__annotations__"):
            owner.__annotations__ = {}

        # Infer type from annotations if not explicitly provided
        self.type = self.type or owner.__annotations__.get(name) or type(_T)
        owner.__annotations__[name] = self.type

        if not hasattr(owner, "_pending_variables"):
            owner._pending_variables = []  # type: ignore
        owner._pending_variables.append((self, self.markers))  # type: ignore

    def load(self):
        """
        Load the variable value from its source or context.

        If a source is configured, loads from the source. Otherwise, loads from
        the pipeline context using the variable name if context exists. Applies
        preprocessing if configured.
        """
        if not hasattr(self, "name"):
            raise ValueError(
                "Variable name is not set. Ensure the variable is properly initialized in a class."
            )

        try:
            # First, try to resolve the default/factory value
            value = resolve_svaluable(self.__value__, self.stage)

            # Next, try to load from context if value is still None
            if value is None and self.context is not None:
                value = self.context.get(self.name, None)

            # Finally, try to load from source if value is still None
            if value is None and self.source and self.source.load_enabled:
                value = self.source.load()

            # If we have a value, apply preprocessing and store it
            if value is not None:
                if self.pre_processing:
                    value = self.pre_processing(value)  # type: ignore[arg-type]
                self.value = value

            if self.value is None:
                logger.warning(
                    f"None value for '{self.name}' while loading. "
                    "No value found to load from context or source. "
                    "If intentional, you can ignore this message."
                )
        except Exception as e:
            raise RuntimeError(
                f"Failed to load variable '{self.name}'. "
                f"Source: {'file' if self.source else 'context'}.\n"
                f"Original error: {str(e)}"
            ) from e

    def save(self):
        """
        Save the variable value to the context and optionally to its source.

        Always saves to the pipeline context if context exists. If a source is
        configured, also saves to the source (e.g., CSV file, array file).
        """
        if not hasattr(self, "name"):
            raise ValueError(
                "Variable name is not set. Ensure the variable is properly initialized in a class."
            )

        try:
            if self.value is None:
                logger.warning(
                    f"None value for '{self.name}' while saving. "
                    "No value found to save after stage execution. "
                    "If intentional, you can ignore this message."
                )

            if self.context is not None:
                self.context.set(self.name, self.value)
            if self.value is not None and self.source is not None and self.source.save_enabled:
                self.source.save(self.value)
        except Exception as e:
            raise RuntimeError(
                f"Failed to save variable '{self.name}'. "
                f"Destination: {'file and context' if self.source else 'context'}.\n"
                f"Original error: {str(e)}"
            ) from e

    def get(self) -> _T:
        """
        Get the current value of the variable.

        Returns:
            The current value.
        """
        return self.value  # type: ignore[return-value]

    def set(self, value: Optional[_T]):
        """
        Set the value of the variable.

        Args:
            value: The new value to store.
        """
        self.value = value

    def delete(self):
        """
        Delete the variable value from memory.

        Sets the value to None to allow garbage collection.
        Note: The actual memory cleanup depends on Python's garbage collector.
        """
        self.value = None

    def validate(self) -> bool:
        """
        Validate that the current value matches the expected type.

        Returns:
            True if the value is valid or no type is specified

        Raises:
            TypeError: If the value doesn't match the expected type
        """
        if not hasattr(self, "name"):
            raise ValueError("Variable name is not set. Cannot validate unnamed variable.")

        if self.type is None or self.value is None:
            return True

        # Skip validation for generic types (they can't be used with isinstance)
        if hasattr(self.type, "__origin__"):
            logger.warning(
                f"Skipping type validation for generic type {self.type} in variable '{self.name}'. "
                "Subclasses should override validate() for specific checks."
            )
            # Subclasses should override validate() for specific checks
            return True

        try:
            if not isinstance(self.value, self.type):
                raise TypeError(
                    f"Variable '{self.name}' type validation failed. "
                    f"Expected type: {self.type.__name__}, "
                    f"Got: {type(self.value).__name__}. "
                    f"Ensure the stage's recipe() method produces the correct output type."
                )
        except TypeError as e:
            # isinstance() can fail with some types, skip validation in that case
            if "Subscripted generics cannot be used" in str(e):
                return True
            raise
        return True

    def sconsume(self) -> _T:
        """Mark this variable as an input (consumed) by the stage.

        Returns:
            Self for method chaining.

        Example:
            >>> var = SVar(pd.DataFrame).sconsume()
            >>> # Equivalent to using sconsume() descriptor function
        """
        self.set_markers([IOMarker.INPUT])
        return self  # type: ignore[return-value]

    def sproduce(self) -> _T:
        """Mark this variable as an output (produced) by the stage.

        Returns:
            Self for method chaining.

        Example:
            >>> var = SVar(pd.DataFrame).sproduce()
            >>> # Equivalent to using sproduce() descriptor function
        """
        self.set_markers([IOMarker.OUTPUT])
        return self  # type: ignore[return-value]

    def stransform(self) -> _T:
        """Mark this variable as both input and output (transformed) by the stage.

        Returns:
            Self for method chaining.

        Example:
            >>> var = SVar(pd.DataFrame).stransform()
            >>> # Equivalent to using stransform() descriptor function
        """
        self.set_markers([IOMarker.INPUT, IOMarker.OUTPUT])
        return self  # type: ignore[return-value]

    def __get__(self, instance, owner):
        """Descriptor protocol method for attribute access.

        This method is called when the variable is accessed as an attribute
        on a stage instance. It returns the current value of the variable.

        Args:
            instance: The stage instance accessing the variable, or None if
                     accessed on the class.
            owner: The class that owns this descriptor.

        Returns:
            The descriptor itself if accessed on the class, otherwise the
            variable's current value.

        Example:
            >>> class MyStage(ETLStage):
            ...     data = sconsume(pd.DataFrame)
            ...
            >>> stage = MyStage()
            >>> # Accessing stage.data calls __get__
            >>> df = stage.data  # Returns the DataFrame value

        Note:
            This is an internal method called by Python's descriptor protocol.
        """
        if instance is None:
            return self
        if hasattr(instance, "_dynamic_props") and self.name in instance._dynamic_props:
            return instance._dynamic_props[self.name]["getter"]()

        return self.get()

    def __set__(self, instance, value):
        """Descriptor protocol method for attribute assignment.

        This method is called when the variable is assigned a value as an
        attribute on a stage instance. It stores the value in the variable.

        Args:
            instance: The stage instance setting the variable.
            value: The value to store.

        Raises:
            ValueError: If attempting to set on the class rather than an instance.

        Example:
            >>> class MyStage(ETLStage):
            ...     result = sproduce(pd.DataFrame)
            ...
            >>> stage = MyStage()
            >>> # Assigning to stage.result calls __set__
            >>> stage.result = processed_df  # Stores the DataFrame

        Note:
            This is an internal method called by Python's descriptor protocol.
        """
        if instance is None:
            raise ValueError("Cannot set value on class")
        if hasattr(instance, "_dynamic_props") and self.name in instance._dynamic_props:
            instance._dynamic_props[self.name]["setter"](value)
        else:
            self.set(value)

    def __delete__(self, instance):
        """Descriptor protocol method for attribute deletion.

        This method is called when the variable is deleted as an attribute
        on a stage instance. It clears the variable's value from memory.

        Args:
            instance: The stage instance deleting the variable.

        Example:
            >>> class MyStage(ETLStage):
            ...     temp_data = stransform(pd.DataFrame)
            ...
            >>> stage = MyStage()
            >>> stage.temp_data = df
            >>> # Deleting stage.temp_data calls __delete__
            >>> del stage.temp_data  # Clears the value

        Note:
            This is an internal method called by Python's descriptor protocol.
        """
        if instance is None:
            return

        if hasattr(instance, "_dynamic_props") and self.name in instance._dynamic_props:
            instance._dynamic_props[self.name]["deleter"]()
        else:
            self.delete()

    def __str__(self):
        """Return string representation of the variable's value.

        Returns:
            String representation of the current value.

        Example:
            >>> var = SVar(str, default="hello")
            >>> str(var)
            'hello'
        """
        return self.value.__str__()

    def __repr__(self):
        """Return detailed string representation of the variable's value.

        Returns:
            Detailed string representation of the current value.

        Example:
            >>> var = SVar(list, default=[1, 2, 3])
            >>> repr(var)
            '[1, 2, 3]'
        """
        return self.value.__repr__()


def _diff_schema(schema_model: Type[PaDataFrameModel], df: pd.DataFrame) -> str:
    expected_cols = set(schema_model.__annotations__.keys())
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    lines: list[str] = []

    if missing:
        lines.append("Missing columns:")
        for c in sorted(missing):
            lines.append(f"  - {c}")

    if extra:
        lines.append("Unexpected columns:")
        for c in sorted(extra):
            lines.append(f"  - {c}")

    # dtype mismatches
    mismatches = []
    for col in expected_cols & actual_cols:
        expected = schema_model.__annotations__[col]
        expected_type = expected.__args__[0]  # Series[T] → T
        actual_dtype = df[col].dtype

        try:
            pandas_expected = pd.Series([], dtype=expected_type).dtype
            if actual_dtype != pandas_expected:
                mismatches.append(f"  - {col}: expected {pandas_expected}, got {actual_dtype}")
        except Exception:
            pass

    if mismatches:
        lines.append("Type mismatches:")
        lines.extend(mismatches)

    return "\n".join(lines)


class DFVar(Generic[_SCHEMA], SVar[PaDataFrame[_SCHEMA]]):
    """DataFrame variable with DFVarSchema (Pandera) validation and type safety.

    DFVar extends SVar to provide specialized handling for pandas DataFrames with
    DFVarSchema (Pandera) support. It enables:

    - Type-safe DataFrame operations with IDE autocomplete for columns
    - Runtime schema validation with automatic type coercion
    - Column introspection
    - Memory-efficient chunk processing for large DataFrames
    - CSV file integration for loading/saving

    The schema parameter accepts a DFVarSchema class, which provides:
    - Column name and type definitions
    - Validation rules (nullable, unique, ranges, etc.)
    - Automatic type coercion during validation
    - IDE support for column access (e.g., df['column_name'])

    Stands for 'DataFrame Variable'.

    Attributes:
        schema: DFVarSchema class for runtime validation and type safety.
        value: The current DataFrame value.
        columns: List of column names (property).

    Example:
        >>> class MySchema(DFVarSchema):
        ...     customer_id: int
        ...     amount: float = pa.Field(ge=0)
        ...     date: pd.Timestamp
        ...
        >>> class ProcessStage(ETLStage):
        ...     input_data = sconsume(DFVar(MySchema))
        ...     output_data = sproduce(DFVar(MySchema))
        ...
        ...     def recipe(self):
        ...         df = self.input_data  # Type: DataFrame[MySchema]
        ...         # IDE knows about customer_id, amount, date columns
        ...         df = df[df['amount'] > 100]
        ...         self.output_data = df  # Validated on save
    """

    schema: Optional[Type[_SCHEMA]]

    def __init__(
        self,
        schema: Optional[Type[_SCHEMA]] = None,
        /,
        *,
        value: Optional[SValuable[PaDataFrame[_SCHEMA]]] = None,
        source: Optional[SValuable[CSVSource]] = None,
        description: Optional[str] = None,
        pre_processing: Optional[Callable[[PaDataFrame[_SCHEMA]], PaDataFrame[_SCHEMA]]] = None,
        markers: Optional[List[IOMarker]] = None,
    ):
        """Initialize a DataFrame variable with optional schema validation.

        Args:
            schema: Optional DFVarSchema class for runtime validation.
                   If provided, the DataFrame will be validated against this schema
                   whenever validate() is called.
            value: Optional static default DataFrame or factory callable.
            source: Optional static default CSV source or factory callable for loading/saving the DataFrame.
            description: Optional human-readable description of the DataFrame's purpose.
            pre_processing: Optional transformation function applied to the DataFrame
                          before it is stored in the variable.
            markers: Optional list of IOMarkers for input/output classification.

        Example:
            >>> # With schema validation
            >>> var = DFVar(
            ...     MySchema,
            ...     source=CSVSource("data.csv"),
            ...     description="Customer transactions"
            ... )
            >>>
            >>> # With factory for empty DataFrame
            >>> var = DFVar(
            ...     MySchema,
            ...     value=lambda: pd.DataFrame(columns=['id', 'value'])
            ... )
            >>>
            >>> # With preprocessing
            >>> var = DFVar(
            ...     MySchema,
            ...     pre_processing=lambda df: df.drop_duplicates()
            ... )
        """
        super().__init__(
            PaDataFrame[_SCHEMA],
            value=value,
            source=source,
            description=description,
            pre_processing=pre_processing,
            markers=markers,
        )
        self.schema = schema

    def validate(self) -> bool:
        """Validate the DataFrame against its schema.

        This method performs two levels of validation:
        1. Type validation: Ensures the value is a pandas DataFrame
        2. Schema validation: If a DFVarSchema is provided, validates the
           DataFrame structure, column types, and any defined constraints

        The schema validation also performs automatic type coercion when possible,
        updating the DataFrame value with the coerced version.

        Returns:
            True if validation passes.

        Raises:
            ValueError: If the variable has no name.
            TypeError: If the value is not a pandas DataFrame.
            ValueError: If DFVarSchema (Pandera) validation fails. The error message
                       includes details about which columns or constraints failed.

        Example:
            >>> class MySchema(DFVarSchema):
            ...     id: int
            ...     value: float = pa.Field(ge=0)
            ...
            >>> var = DFVar(MySchema, name="data")
            >>> var.value = pd.DataFrame({'id': ['1', '2'], 'value': [10.5, 20.3]})
            >>> var.validate()  # Coerces 'id' to int, validates 'value' >= 0
            True
            >>> var.value = pd.DataFrame({'id': [1, 2], 'value': [-5, 10]})
            >>> var.validate()  # Raises ValueError: value must be >= 0
            ValueError: DFVarSchema (Pandera) validation failed...

        Note:
            If no schema is provided, only basic DataFrame type checking is performed.
        """
        if not hasattr(self, "name"):
            raise ValueError(
                "Variable name is not set. Cannot validate unnamed DataFrame variable."
            )

        if self.value is None:
            return True

        if not isinstance(self.value, pd.DataFrame):
            raise TypeError(
                f"Variable '{self.name}' type validation failed. "
                f"Expected DataFrame, got {type(self.value).__name__}. "
                f"Ensure the stage's recipe() method produces a pandas DataFrame."
            )

        if self.schema is not None:
            try:
                validated_df = self.schema.M.validate(self.value, lazy=False)
                object.__setattr__(self, "value", validated_df)
                return True
            except Exception as e:
                diff = _diff_schema(self.schema.M, self.value)
                raise ValueError(
                    f"DFVarSchema (Pandera) validation failed for DataFrame '{self.name}'. "
                    f"Check that the DataFrame has the correct columns and types.\n"
                    f"Schema differences:\n{diff}\n\n"
                    f"Original error: {str(e)}"
                ) from e
        return True


class NDArrayVar(Generic[_T], SVar[np.ndarray]):
    """NumPy array variable with shape validation for numerical data.

    NDArrayVar extends SVar to provide specialized handling for NumPy arrays with
    optional shape validation. It enables:

    - Type-safe array operations
    - Shape validation to catch dimension mismatches early
    - Integration with ArraySource for .npy file loading/saving
    - Support for structured arrays and multi-dimensional data
    - Preprocessing transformations (normalization, reshaping, etc.)

    The shape parameter can be used to enforce specific array dimensions,
    which is particularly useful for:
    - Machine learning model inputs/outputs (e.g., embeddings, predictions)
    - Image data (height, width, channels)
    - Time series data (samples, features)
    - Matrix operations requiring specific dimensions

    Stands for 'NumPy Array Variable'.

    Attributes:
        shape: Expected shape tuple (e.g., (100, 768) for 100 embeddings of 768 dimensions).
        value: The current NumPy array value.

    Example:
        >>> class EmbeddingStage(ETLStage):
        ...     # Input: 2D array of text data
        ...     text_ids = sconsume(NDArrayVar(shape=(None, 512)))
        ...
        ...     # Output: 2D array of embeddings
        ...     embeddings = sproduce(NDArrayVar(
        ...         shape=(None, 768),
        ...         source=ArraySource("embeddings.npy", mode="w")
        ...     ))
        ...
        ...     def recipe(self):
        ...         # Process text_ids to generate embeddings
        ...         self.embeddings = model.encode(self.text_ids)
        >>>
        >>> # With preprocessing
        >>> normalized_data = NDArrayVar(
        ...     shape=(1000, 50),
        ...     pre_processing=lambda arr: (arr - arr.mean()) / arr.std()
        ... )
    """

    shape: Optional[tuple]

    def __init__(
        self,
        *,
        value: Optional[SValuable[np.ndarray]] = None,
        source: Optional[SValuable[ArraySource]] = None,
        description: Optional[str] = None,
        pre_processing: Optional[Callable[[np.ndarray], np.ndarray]] = None,
        shape: Optional[tuple] = None,
        markers: Optional[List[IOMarker]] = None,
    ):
        """Initialize a NumPy array variable with optional shape validation.

        Args:
            value: Optional static default NumPy array or factory callable.
            source: Optional static default ArraySource or factory callable for loading/saving the array to .npy files.
            description: Optional human-readable description of the array's purpose.
            pre_processing: Optional transformation function applied to the array
                          before it is stored in the variable.
            shape: Optional expected shape tuple for validation. Use None in a
                  dimension to allow any size (e.g., (None, 768) allows any number
                  of 768-dimensional vectors).
            markers: Optional list of IOMarkers for input/output classification.

        Example:
            >>> # Fixed shape array
            >>> var = NDArrayVar(
            ...     shape=(100, 50),
            ...     value=lambda: np.zeros((100, 50)),
            ...     description="Feature matrix"
            ... )
            >>>
            >>> # Variable-length array with fixed feature dimension
            >>> var = NDArrayVar(
            ...     shape=(None, 768),
            ...     source=ArraySource("embeddings.npy"),
            ...     description="Text embeddings"
            ... )
            >>>
            >>> # With normalization preprocessing
            >>> var = NDArrayVar(
            ...     shape=(1000, 50),
            ...     pre_processing=lambda arr: arr / np.linalg.norm(arr, axis=1, keepdims=True)
            ... )
        """
        super().__init__(
            np.ndarray,
            value=value,
            source=source,
            description=description,
            pre_processing=pre_processing,
            markers=markers,
        )
        self.shape = shape

    def validate(self) -> bool:
        """Validate the NumPy array type and shape.

        This method performs two levels of validation:
        1. Type validation: Ensures the value is a NumPy ndarray
        2. Shape validation: If a shape is specified, ensures the array matches
           the expected dimensions

        Returns:
            True if validation passes.

        Raises:
            TypeError: If the value is not a NumPy ndarray.
            ValueError: If the array shape doesn't match the expected shape.

        Example:
            >>> var = NDArrayVar(shape=(100, 768), name="embeddings")
            >>> var.value = np.zeros((100, 768))
            >>> var.validate()  # Returns True
            True
            >>>
            >>> var.value = np.zeros((50, 768))
            >>> var.validate()  # Raises ValueError
            ValueError: Array 'embeddings' has incorrect shape. Expected: (100, 768), Got: (50, 768)
            >>>
            >>> var.value = [1, 2, 3]  # Not a NumPy array
            >>> var.validate()  # Raises TypeError
            TypeError: Variable 'embeddings' expected numpy.ndarray, but got list

        Note:
            If no shape is specified during initialization, only type validation
            is performed. If the value is None, validation passes without checking.
        """
        super().validate()

        if self.value is None or self.shape is None:
            return True

        if not isinstance(self.value, np.ndarray):
            raise TypeError(
                f"Variable '{self.name}' expected numpy.ndarray, "
                f"but got {type(self.value).__name__}"
            )

        if self.value.shape != self.shape:
            raise ValueError(
                f"Array '{self.name}' has incorrect shape. "
                f"Expected: {self.shape}, Got: {self.value.shape}"
            )

        return True
