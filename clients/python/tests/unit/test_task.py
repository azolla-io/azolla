"""Unit tests for task functionality."""
import pytest
from typing import Optional
from pydantic import BaseModel

from azolla import azolla_task, Task, ValidationError, TaskContext
from azolla.types import TaskStatus

# Test tasks for decorator approach
@azolla_task
async def simple_task(name: str, count: int = 1) -> dict:
    """Simple task for testing decorator."""
    return {"message": f"Hello {name}!", "count": count}

@azolla_task  
async def failing_task(should_fail: bool = True) -> dict:
    """Task that can fail for testing error handling."""
    if should_fail:
        raise ValueError("Task intentionally failed")
    return {"status": "success"}

# Test task for explicit approach
class ExplicitTask(Task):
    """Test task using explicit class approach."""
    
    class Args(BaseModel):
        value: int
        multiplier: float = 2.0
        
    async def execute(self, args: Args, context: Optional[TaskContext] = None) -> dict:
        result = args.value * args.multiplier
        return {"input": args.value, "multiplier": args.multiplier, "result": result}

class TestTaskDecorator:
    """Test azolla_task decorator functionality."""
    
    def test_decorator_creates_task_attributes(self) -> None:
        """Test that decorator creates proper task attributes."""
        assert hasattr(simple_task, '__azolla_task_class__')
        assert hasattr(simple_task, '__azolla_args_model__')
        assert hasattr(simple_task, '__azolla_task_instance__')
        
        task_class = simple_task.__azolla_task_class__
        assert issubclass(task_class, Task)
    
    def test_decorator_preserves_function_metadata(self) -> None:
        """Test that decorator preserves original function metadata."""
        assert simple_task.__name__ == "simple_task"
        assert "Simple task for testing" in simple_task.__doc__
    
    def test_task_name_generation(self) -> None:
        """Test that task names are generated correctly."""
        task_instance = simple_task.__azolla_task_instance__
        assert task_instance.name() == "simple_task"
    
    async def test_decorated_function_still_callable(self) -> None:
        """Test that decorated function can still be called directly."""
        result = await simple_task("World", count=3)
        assert result == {"message": "Hello World!", "count": 3}
    
    async def test_task_execution_via_instance(self) -> None:
        """Test task execution through task instance."""
        task_instance = simple_task.__azolla_task_instance__
        
        # Test with dict args
        result = await task_instance._execute_with_casting({"name": "Test", "count": 5})
        assert result == {"message": "Hello Test!", "count": 5}
        
        # Test with list args
        result = await task_instance._execute_with_casting([{"name": "List", "count": 2}])
        assert result == {"message": "Hello List!", "count": 2}
    
    async def test_task_argument_validation(self) -> None:
        """Test that task arguments are properly validated."""
        task_instance = simple_task.__azolla_task_instance__
        
        # Valid arguments
        await task_instance._execute_with_casting({"name": "Valid"})
        
        # Invalid arguments - missing required field
        with pytest.raises(ValidationError):
            await task_instance._execute_with_casting({"count": 5})  # Missing 'name'
        
        # Invalid arguments - wrong type  
        with pytest.raises(ValidationError):
            await task_instance._execute_with_casting({"name": "Test", "count": "invalid"})
    
    async def test_error_handling_in_decorated_task(self) -> None:
        """Test that errors in decorated tasks are properly handled."""
        task_instance = failing_task.__azolla_task_instance__
        
        # Test task that should fail
        with pytest.raises(ValueError, match="Task intentionally failed"):
            await task_instance._execute_with_casting({"should_fail": True})
        
        # Test task that should succeed
        result = await task_instance._execute_with_casting({"should_fail": False})
        assert result == {"status": "success"}

class TestExplicitTask:
    """Test explicit Task class implementation."""
    
    def test_explicit_task_name(self) -> None:
        """Test that explicit task generates correct name."""
        task = ExplicitTask()
        assert task.name() == "explicit"  # "ExplicitTask" -> "explicit"
    
    def test_explicit_task_args_parsing(self) -> None:
        """Test argument parsing in explicit tasks."""
        task = ExplicitTask()
        
        # Test dict parsing
        args = task.parse_args({"value": 10, "multiplier": 3.0})
        assert args.value == 10
        assert args.multiplier == 3.0
        
        # Test list parsing
        args = task.parse_args([{"value": 5}])
        assert args.value == 5
        assert args.multiplier == 2.0  # Default value
        
        # Test validation error
        with pytest.raises(ValidationError):
            task.parse_args({"value": "invalid"})  # Wrong type
    
    async def test_explicit_task_execution(self) -> None:
        """Test execution of explicit task."""
        task = ExplicitTask()
        
        # Test with context
        context = TaskContext(task_id="test-123", attempt_number=1)
        result = await task._execute_with_casting({"value": 6, "multiplier": 1.5}, context)
        
        expected = {"input": 6, "multiplier": 1.5, "result": 9.0}
        assert result == expected
    
    async def test_explicit_task_default_values(self) -> None:
        """Test that default values work in explicit tasks."""
        task = ExplicitTask()
        
        # Only provide required field
        result = await task._execute_with_casting({"value": 4})
        expected = {"input": 4, "multiplier": 2.0, "result": 8.0}
        assert result == expected

class TestTaskContext:
    """Test TaskContext functionality."""
    
    def test_task_context_creation(self) -> None:
        """Test TaskContext can be created with required fields."""
        context = TaskContext(task_id="test-456", attempt_number=2, max_attempts=3)
        
        assert context.task_id == "test-456"
        assert context.attempt_number == 2
        assert context.max_attempts == 3
    
    def test_is_final_attempt(self) -> None:
        """Test final attempt detection."""
        # Final attempt
        context = TaskContext(task_id="test", attempt_number=3, max_attempts=3)
        assert context.is_final_attempt() is True
        
        # Not final attempt
        context = TaskContext(task_id="test", attempt_number=2, max_attempts=3)
        assert context.is_final_attempt() is False
        
        # No max attempts set
        context = TaskContext(task_id="test", attempt_number=5)
        assert context.is_final_attempt() is False