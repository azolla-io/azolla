"""Task base class and decorator implementation."""

import functools
import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Union, get_type_hints

from pydantic import BaseModel, create_model
from pydantic import ValidationError as PydanticValidationError

from azolla.exceptions import ValidationError


class Task(ABC):
    """Base class for all tasks with automatic argument casting."""

    _args_type: Optional[type[BaseModel]] = None

    def __init_subclass__(cls) -> None:
        """set up automatic Args type detection."""
        if hasattr(cls, "Args"):
            cls._args_type = cls.Args
        super().__init_subclass__()

    @abstractmethod
    async def execute(self, args: Any) -> Any:
        """Execute the task with typed arguments."""
        pass

    async def _execute_with_casting(
        self,
        raw_args: Union[dict[str, Any], list[Any]],
    ) -> Any:
        """Internal method that handles automatic casting."""
        if self._args_type:
            try:
                typed_args = self.parse_args(raw_args)
            except Exception as e:
                raise ValidationError(f"Failed to parse task arguments: {e}") from e
            return await self.execute(typed_args)
        # No typed Args model: pass through raw args unchanged
        return await self.execute(raw_args)

    @classmethod
    def parse_args(cls, json_args: Union[list[Any], dict[str, Any]]) -> BaseModel:
        """Parse JSON arguments into typed arguments."""
        if not cls._args_type:
            raise ValidationError("Task has no Args type defined")

        try:
            if isinstance(json_args, list):
                if not json_args:
                    return cls._args_type()
                if len(json_args) == 1 and isinstance(json_args[0], dict):
                    return cls._args_type.model_validate(json_args[0])
                # Map positional list to fields by order
                field_names = list(cls._args_type.model_fields.keys())
                if len(json_args) > len(field_names):
                    raise ValidationError(
                        f"Too many positional arguments: expected at most {len(field_names)}, got {len(json_args)}"
                    )
                data = {name: json_args[i] for i, name in enumerate(field_names[: len(json_args)])}
                return cls._args_type.model_validate(data)
            else:
                # dict - treat as keyword arguments
                return cls._args_type.model_validate(json_args)
        except PydanticValidationError as e:
            raise ValidationError(f"Argument validation failed: {e}") from e

    def name(self) -> str:
        """Task name for registration."""
        class_name = self.__class__.__name__
        if class_name.endswith("Task"):
            return class_name[:-4].lower()
        return class_name.lower()


def azolla_task(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that converts async functions into Task classes."""

    if not inspect.iscoroutinefunction(func):
        raise ValueError("azolla_task can only be applied to async functions")

    # Extract function signature
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)

    # Create Pydantic model from function parameters
    fields = {}
    for param_name, param in sig.parameters.items():
        param_type = type_hints.get(param_name, str)

        if param.default == param.empty:
            default_value = ...  # Required field
        else:
            default_value = param.default

        fields[param_name] = (param_type, default_value)

    # Generate Args model with proper field annotations
    # Use direct field dictionary format that create_model expects
    model_fields = {}
    for name, (field_type, default_value) in fields.items():
        model_fields[name] = (field_type, default_value)

    args_model_name = f"{func.__name__.title().replace('_', '')}Args"
    # Type ignore the create_model call as pydantic's type annotation is incomplete
    args_model = create_model(args_model_name, **model_fields)  # type: ignore[call-overload]

    # Store original function separately
    original_func = func

    # Generate Task class
    class GeneratedTask(Task):
        Args = args_model
        _original_func = staticmethod(original_func)  # Store as staticmethod to avoid self issues

        async def execute(self, args: Any) -> Any:
            # Convert args back to function parameters
            kwargs = args.model_dump()
            return await self._original_func(**kwargs)

        def name(self) -> str:
            return str(original_func.__name__)

    # Create task instance
    task_instance = GeneratedTask()

    # Add special attributes (can't use update_wrapper on task instance)
    task_instance.__name__ = original_func.__name__  # type: ignore[attr-defined]
    task_instance.__azolla_task_class__ = GeneratedTask  # type: ignore[attr-defined]
    task_instance.__azolla_args_model__ = args_model  # type: ignore[attr-defined]

    # Make it callable like the original function for testing
    @functools.wraps(original_func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        return await original_func(*args, **kwargs)

    # Copy task attributes to wrapper
    wrapper.__azolla_task_class__ = GeneratedTask  # type: ignore[attr-defined]
    wrapper.__azolla_args_model__ = args_model  # type: ignore[attr-defined]
    wrapper.__azolla_task_instance__ = task_instance  # type: ignore[attr-defined]

    return wrapper
