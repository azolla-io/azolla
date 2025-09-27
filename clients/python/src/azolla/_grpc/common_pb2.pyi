from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AnyValue(_message.Message):
    __slots__ = ("string_value", "int_value", "double_value", "bool_value", "json_value")
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUE_FIELD_NUMBER: _ClassVar[int]
    string_value: str
    int_value: int
    double_value: float
    bool_value: bool
    json_value: str
    def __init__(self, string_value: _Optional[str] = ..., int_value: _Optional[int] = ..., double_value: _Optional[float] = ..., bool_value: bool = ..., json_value: _Optional[str] = ...) -> None: ...

class RetryPolicyStop(_message.Message):
    __slots__ = ("max_attempts", "max_delay")
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    MAX_DELAY_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    max_delay: float
    def __init__(self, max_attempts: _Optional[int] = ..., max_delay: _Optional[float] = ...) -> None: ...

class RetryPolicyFixedWait(_message.Message):
    __slots__ = ("delay",)
    DELAY_FIELD_NUMBER: _ClassVar[int]
    delay: float
    def __init__(self, delay: _Optional[float] = ...) -> None: ...

class RetryPolicyExponentialWait(_message.Message):
    __slots__ = ("initial_delay", "multiplier", "max_delay")
    INITIAL_DELAY_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    MAX_DELAY_FIELD_NUMBER: _ClassVar[int]
    initial_delay: float
    multiplier: float
    max_delay: float
    def __init__(self, initial_delay: _Optional[float] = ..., multiplier: _Optional[float] = ..., max_delay: _Optional[float] = ...) -> None: ...

class RetryPolicyExponentialJitterWait(_message.Message):
    __slots__ = ("initial_delay", "multiplier", "max_delay")
    INITIAL_DELAY_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    MAX_DELAY_FIELD_NUMBER: _ClassVar[int]
    initial_delay: float
    multiplier: float
    max_delay: float
    def __init__(self, initial_delay: _Optional[float] = ..., multiplier: _Optional[float] = ..., max_delay: _Optional[float] = ...) -> None: ...

class RetryPolicyWait(_message.Message):
    __slots__ = ("fixed", "exponential", "exponential_jitter")
    FIXED_FIELD_NUMBER: _ClassVar[int]
    EXPONENTIAL_FIELD_NUMBER: _ClassVar[int]
    EXPONENTIAL_JITTER_FIELD_NUMBER: _ClassVar[int]
    fixed: RetryPolicyFixedWait
    exponential: RetryPolicyExponentialWait
    exponential_jitter: RetryPolicyExponentialJitterWait
    def __init__(self, fixed: _Optional[_Union[RetryPolicyFixedWait, _Mapping]] = ..., exponential: _Optional[_Union[RetryPolicyExponentialWait, _Mapping]] = ..., exponential_jitter: _Optional[_Union[RetryPolicyExponentialJitterWait, _Mapping]] = ...) -> None: ...

class RetryPolicyRetry(_message.Message):
    __slots__ = ("include_errors", "exclude_errors")
    INCLUDE_ERRORS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_ERRORS_FIELD_NUMBER: _ClassVar[int]
    include_errors: _containers.RepeatedScalarFieldContainer[str]
    exclude_errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, include_errors: _Optional[_Iterable[str]] = ..., exclude_errors: _Optional[_Iterable[str]] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("version", "stop", "wait", "retry")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    WAIT_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    version: int
    stop: RetryPolicyStop
    wait: RetryPolicyWait
    retry: RetryPolicyRetry
    def __init__(self, version: _Optional[int] = ..., stop: _Optional[_Union[RetryPolicyStop, _Mapping]] = ..., wait: _Optional[_Union[RetryPolicyWait, _Mapping]] = ..., retry: _Optional[_Union[RetryPolicyRetry, _Mapping]] = ...) -> None: ...

class StructValue(_message.Message):
    __slots__ = ("json_data",)
    JSON_DATA_FIELD_NUMBER: _ClassVar[int]
    json_data: str
    def __init__(self, json_data: _Optional[str] = ...) -> None: ...

class SuccessResult(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: AnyValue
    def __init__(self, result: _Optional[_Union[AnyValue, _Mapping]] = ...) -> None: ...

class ErrorResult(_message.Message):
    __slots__ = ("type", "message", "data", "retriable")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    RETRIABLE_FIELD_NUMBER: _ClassVar[int]
    type: str
    message: str
    data: str
    retriable: bool
    def __init__(self, type: _Optional[str] = ..., message: _Optional[str] = ..., data: _Optional[str] = ..., retriable: bool = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = ("task_id", "success", "error")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    success: SuccessResult
    error: ErrorResult
    def __init__(self, task_id: _Optional[str] = ..., success: _Optional[_Union[SuccessResult, _Mapping]] = ..., error: _Optional[_Union[ErrorResult, _Mapping]] = ...) -> None: ...

class Task(_message.Message):
    __slots__ = ("task_id", "name", "args", "kwargs", "memory_limit", "cpu_limit")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    CPU_LIMIT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    name: str
    args: str
    kwargs: str
    memory_limit: int
    cpu_limit: int
    def __init__(self, task_id: _Optional[str] = ..., name: _Optional[str] = ..., args: _Optional[str] = ..., kwargs: _Optional[str] = ..., memory_limit: _Optional[int] = ..., cpu_limit: _Optional[int] = ...) -> None: ...
