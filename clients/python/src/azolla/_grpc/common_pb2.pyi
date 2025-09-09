from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
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
    __slots__ = ("type", "message", "code", "stacktrace", "data")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    STACKTRACE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    type: str
    message: str
    code: str
    stacktrace: str
    data: StructValue
    def __init__(self, type: _Optional[str] = ..., message: _Optional[str] = ..., code: _Optional[str] = ..., stacktrace: _Optional[str] = ..., data: _Optional[_Union[StructValue, _Mapping]] = ...) -> None: ...

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
