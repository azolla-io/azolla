"""Task base class and decorator implementation."""
import json
import inspect
import functools
from typing import Any, Dict, List, Optional, Union, get_type_hints, get_origin, get_args
from abc import ABC, abstractmethod
from pydantic import BaseModel, create_model, ValidationError as PydanticValidationError

from azolla.types import TaskContext, TaskResult, TaskStatus
from azolla.exceptions import TaskError, ValidationError

class Task(ABC):
    """Base class for all tasks with automatic argument casting."""
    
    def __init_subclass__(cls) -> None:
        """Set up automatic Args type detection."""
        if hasattr(cls, 'Args'):
            cls._args_type = cls.Args
        super().__init_subclass__()
    
    @abstractmethod
    async def execute(self, args: BaseModel, context: Optional[TaskContext] = None) -> Any:
        """Execute the task with typed arguments."""
        pass
    
    async def _execute_with_casting(
        self, 
        raw_args: Union[Dict[str, Any], List[Any]], 
        context: Optional[TaskContext] = None
    ) -> Any:
        """Internal method that handles automatic casting."""
        if hasattr(self, '_args_type'):
            try:
                typed_args = self.parse_args(raw_args)
            except Exception as e:
                raise ValidationError(f"Failed to parse task arguments: {e}")
        else:
            typed_args = raw_args
        
        return await self.execute(typed_args, context)
    
    @classmethod
    def parse_args(cls, json_args: Union[List[Any], Dict[str, Any]]) -> BaseModel:
        """Parse JSON arguments into typed arguments."""
        if not hasattr(cls, '_args_type'):
            raise ValidationError("Task has no Args type defined")
            
        try:
            if isinstance(json_args, list):
                if not json_args:
                    return cls._args_type()
                elif len(json_args) == 1:
                    return cls._args_type.model_validate(json_args[0])
                else:
                    # Multiple args - treat as positional arguments
                    return cls._args_type.model_validate(json_args)
            else:
                # Dict - treat as keyword arguments
                return cls._args_type.model_validate(json_args)
        except PydanticValidationError as e:
            raise ValidationError(f"Argument validation failed: {e}")
    
    def name(self) -> str:
        """Task name for registration."""
        class_name = self.__class__.__name__
        if class_name.endswith('Task'):
            return class_name[:-4].lower()
        return class_name.lower()

def azolla_task(func):
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
    
    # Generate Args model
    args_model_name = f"{func.__name__.title().replace('_', '')}Args"
    args_model = create_model(args_model_name, **fields)
    
    # Store original function separately
    original_func = func
    
    # Generate Task class
    class GeneratedTask(Task):
        Args = args_model
        _original_func = staticmethod(original_func)  # Store as staticmethod to avoid self issues
        
        async def execute(
            self, 
            args: args_model, 
            context: Optional[TaskContext] = None
        ) -> Any:
            # Convert args back to function parameters
            kwargs = args.model_dump()
            return await self._original_func(**kwargs)
        
        def name(self) -> str:
            return original_func.__name__
    
    # Create task instance
    task_instance = GeneratedTask()
    
    # Copy function metadata and add special attributes
    functools.update_wrapper(task_instance, original_func)
    task_instance.__name__ = original_func.__name__
    task_instance.__azolla_task_class__ = GeneratedTask
    task_instance.__azolla_args_model__ = args_model
    
    # Make it callable like the original function for testing
    @functools.wraps(original_func)
    async def wrapper(*args, **kwargs):
        return await original_func(*args, **kwargs)
    
    # Copy task attributes to wrapper
    wrapper.__azolla_task_class__ = GeneratedTask
    wrapper.__azolla_args_model__ = args_model
    wrapper.__azolla_task_instance__ = task_instance
    
    return wrapper