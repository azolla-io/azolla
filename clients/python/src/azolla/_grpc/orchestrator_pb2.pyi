import common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateTaskRequest(_message.Message):
    __slots__ = ("name", "domain", "retry_policy", "args", "kwargs", "flow_instance_id", "shepherd_group")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    RETRY_POLICY_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    FLOW_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    SHEPHERD_GROUP_FIELD_NUMBER: _ClassVar[int]
    name: str
    domain: str
    retry_policy: str
    args: str
    kwargs: str
    flow_instance_id: str
    shepherd_group: str
    def __init__(self, name: _Optional[str] = ..., domain: _Optional[str] = ..., retry_policy: _Optional[str] = ..., args: _Optional[str] = ..., kwargs: _Optional[str] = ..., flow_instance_id: _Optional[str] = ..., shepherd_group: _Optional[str] = ...) -> None: ...

class CreateTaskResponse(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class WaitForTaskRequest(_message.Message):
    __slots__ = ("task_id", "domain")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    domain: str
    def __init__(self, task_id: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class WaitForTaskResponse(_message.Message):
    __slots__ = ("status", "result", "error")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    status: str
    result: str
    error: str
    def __init__(self, status: _Optional[str] = ..., result: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class CreateFlowRequest(_message.Message):
    __slots__ = ("name", "domain", "dag")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    DAG_FIELD_NUMBER: _ClassVar[int]
    name: str
    domain: str
    dag: str
    def __init__(self, name: _Optional[str] = ..., domain: _Optional[str] = ..., dag: _Optional[str] = ...) -> None: ...

class CreateFlowResponse(_message.Message):
    __slots__ = ("flow_id",)
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    def __init__(self, flow_id: _Optional[str] = ...) -> None: ...

class WaitForFlowRequest(_message.Message):
    __slots__ = ("flow_id", "domain")
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    flow_id: str
    domain: str
    def __init__(self, flow_id: _Optional[str] = ..., domain: _Optional[str] = ...) -> None: ...

class WaitForFlowResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class PublishTaskEventRequest(_message.Message):
    __slots__ = ("task_instance_id", "domain", "event_type", "metadata")
    TASK_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    task_instance_id: str
    domain: str
    event_type: int
    metadata: str
    def __init__(self, task_instance_id: _Optional[str] = ..., domain: _Optional[str] = ..., event_type: _Optional[int] = ..., metadata: _Optional[str] = ...) -> None: ...

class PublishTaskEventResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class PublishFlowEventRequest(_message.Message):
    __slots__ = ("flow_instance_id", "domain", "event_type", "metadata")
    FLOW_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    flow_instance_id: str
    domain: str
    event_type: int
    metadata: str
    def __init__(self, flow_instance_id: _Optional[str] = ..., domain: _Optional[str] = ..., event_type: _Optional[int] = ..., metadata: _Optional[str] = ...) -> None: ...

class PublishFlowEventResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class ClientMsg(_message.Message):
    __slots__ = ("hello", "ack", "status", "task_result")
    HELLO_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TASK_RESULT_FIELD_NUMBER: _ClassVar[int]
    hello: Hello
    ack: Ack
    status: Status
    task_result: _common_pb2.TaskResult
    def __init__(self, hello: _Optional[_Union[Hello, _Mapping]] = ..., ack: _Optional[_Union[Ack, _Mapping]] = ..., status: _Optional[_Union[Status, _Mapping]] = ..., task_result: _Optional[_Union[_common_pb2.TaskResult, _Mapping]] = ...) -> None: ...

class ServerMsg(_message.Message):
    __slots__ = ("task", "ping")
    TASK_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    task: _common_pb2.Task
    ping: Ping
    def __init__(self, task: _Optional[_Union[_common_pb2.Task, _Mapping]] = ..., ping: _Optional[_Union[Ping, _Mapping]] = ...) -> None: ...

class Hello(_message.Message):
    __slots__ = ("shepherd_uuid", "max_concurrency", "domain", "shepherd_group")
    SHEPHERD_UUID_FIELD_NUMBER: _ClassVar[int]
    MAX_CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    SHEPHERD_GROUP_FIELD_NUMBER: _ClassVar[int]
    shepherd_uuid: str
    max_concurrency: int
    domain: str
    shepherd_group: str
    def __init__(self, shepherd_uuid: _Optional[str] = ..., max_concurrency: _Optional[int] = ..., domain: _Optional[str] = ..., shepherd_group: _Optional[str] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ("current_load", "available_capacity")
    CURRENT_LOAD_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    current_load: int
    available_capacity: int
    def __init__(self, current_load: _Optional[int] = ..., available_capacity: _Optional[int] = ...) -> None: ...

class Ping(_message.Message):
    __slots__ = ("timestamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    def __init__(self, timestamp: _Optional[int] = ...) -> None: ...
