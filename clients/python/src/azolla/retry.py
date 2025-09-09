"""Retry policy implementation for Azolla tasks."""
import math
import random
from typing import List, Optional, Type, Union, Any, Dict
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, field_serializer

class BackoffStrategy(ABC):
    """Base class for backoff strategies."""
    
    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """Get delay in seconds for the given attempt number."""
        pass

class ExponentialBackoff(BackoffStrategy, BaseModel):
    """Exponential backoff with jitter."""
    initial: float = Field(default=1.0, gt=0)
    multiplier: float = Field(default=2.0, gt=1.0)
    max_delay: float = Field(default=300.0, gt=0)
    jitter: bool = Field(default=True)
    
    def get_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        delay = self.initial * (self.multiplier ** (attempt - 1))
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            # Add up to 25% jitter
            jitter_factor = 1.0 + (random.random() - 0.5) * 0.5
            delay *= jitter_factor
            
        return max(0, delay)

class LinearBackoff(BackoffStrategy, BaseModel):
    """Linear backoff strategy."""
    initial: float = Field(default=1.0, gt=0)
    increment: float = Field(default=1.0, gt=0)
    max_delay: float = Field(default=300.0, gt=0)
    
    def get_delay(self, attempt: int) -> float:
        """Calculate linear backoff delay."""
        delay = self.initial + (self.increment * (attempt - 1))
        return min(delay, self.max_delay)

class FixedBackoff(BackoffStrategy, BaseModel):
    """Fixed delay backoff strategy."""
    delay: float = Field(default=1.0, gt=0)
    
    def get_delay(self, attempt: int) -> float:
        """Return fixed delay."""
        return self.delay

class RetryPolicy(BaseModel):
    """Configuration for task retry behavior."""
    max_attempts: int = Field(default=3, ge=1)
    backoff: BackoffStrategy = Field(default_factory=ExponentialBackoff)
    retry_on: List[Union[str, Type[Exception]]] = Field(default_factory=list)
    stop_on_codes: List[str] = Field(default_factory=list)
    
    model_config = {"arbitrary_types_allowed": True}
    
    @field_serializer('retry_on')
    def serialize_retry_on(self, value: List[Union[str, Type[Exception]]]) -> List[str]:
        """Serialize retry_on list, converting class types to their names."""
        result = []
        for item in value:
            if isinstance(item, str):
                result.append(item)
            elif isinstance(item, type):
                result.append(item.__name__)
            else:
                result.append(str(item))
        return result
        
    def should_retry(
        self, 
        attempt: int, 
        error: Exception, 
        error_code: Optional[str] = None
    ) -> bool:
        """Determine if task should be retried."""
        # Check if we've exceeded max attempts
        if attempt >= self.max_attempts:
            return False
            
        # Check if error code is in stop list
        if error_code and error_code in self.stop_on_codes:
            return False
            
        # Check if error type should be retried
        if self.retry_on:
            error_matches = any(
                (isinstance(retry_type, str) and retry_type == error.__class__.__name__) or
                (isinstance(retry_type, type) and isinstance(error, retry_type))
                for retry_type in self.retry_on
            )
            return error_matches
            
        # Default: retry on retryable task errors
        from azolla.exceptions import TaskError
        if isinstance(error, TaskError):
            return error.retryable
            
        # If no specific retry_on list and not a TaskError, retry by default
        # (changed from previous behavior)
        if not self.retry_on:
            return True
            
        # Don't retry other exceptions by default if retry_on is specified
        return False
        
    def get_delay(self, attempt: int) -> float:
        """Get delay before next retry attempt."""
        return self.backoff.get_delay(attempt)