"""Unit tests for retry policy functionality."""
import pytest
from azolla.retry import RetryPolicy, ExponentialBackoff, LinearBackoff, FixedBackoff
from azolla.exceptions import TaskError, ValidationError

class TestBackoffStrategies:
    """Test backoff strategy implementations."""
    
    def test_exponential_backoff(self) -> None:
        """Test exponential backoff calculation."""
        backoff = ExponentialBackoff(initial=1.0, multiplier=2.0, max_delay=30.0, jitter=False)
        
        # Test exponential growth
        assert backoff.get_delay(1) == 1.0  # 1.0 * 2^0
        assert backoff.get_delay(2) == 2.0  # 1.0 * 2^1
        assert backoff.get_delay(3) == 4.0  # 1.0 * 2^2
        assert backoff.get_delay(4) == 8.0  # 1.0 * 2^3
        
        # Test max delay cap
        assert backoff.get_delay(10) == 30.0  # Should be capped
    
    def test_exponential_backoff_with_jitter(self) -> None:
        """Test exponential backoff with jitter."""
        backoff = ExponentialBackoff(initial=1.0, multiplier=2.0, jitter=True)
        
        # With jitter, delays should vary but be in expected range
        delays = [backoff.get_delay(3) for _ in range(10)]
        base_delay = 4.0  # 1.0 * 2^2
        
        # All delays should be within jitter range (±25% of base)
        min_expected = base_delay * 0.75
        max_expected = base_delay * 1.25
        
        for delay in delays:
            assert min_expected <= delay <= max_expected
    
    def test_linear_backoff(self) -> None:
        """Test linear backoff calculation."""
        backoff = LinearBackoff(initial=2.0, increment=1.5, max_delay=20.0)
        
        # Test linear growth
        assert backoff.get_delay(1) == 2.0      # 2.0 + 1.5 * 0
        assert backoff.get_delay(2) == 3.5      # 2.0 + 1.5 * 1  
        assert backoff.get_delay(3) == 5.0      # 2.0 + 1.5 * 2
        assert backoff.get_delay(4) == 6.5      # 2.0 + 1.5 * 3
        
        # Test max delay cap
        assert backoff.get_delay(20) == 20.0    # Should be capped
    
    def test_fixed_backoff(self) -> None:
        """Test fixed backoff calculation."""
        backoff = FixedBackoff(delay=5.0)
        
        # Should always return the same delay
        assert backoff.get_delay(1) == 5.0
        assert backoff.get_delay(5) == 5.0
        assert backoff.get_delay(100) == 5.0

class TestRetryPolicy:
    """Test retry policy functionality."""
    
    def test_retry_policy_defaults(self) -> None:
        """Test retry policy default values."""
        policy = RetryPolicy()
        
        assert policy.max_attempts == 3
        assert isinstance(policy.backoff, ExponentialBackoff)
        assert policy.retry_on == []
        assert policy.stop_on_codes == []
    
    def test_retry_policy_custom_config(self) -> None:
        """Test retry policy with custom configuration."""
        backoff = LinearBackoff(initial=1.0)
        policy = RetryPolicy(
            max_attempts=5,
            backoff=backoff,
            retry_on=[ValueError, "TimeoutError"],
            stop_on_codes=["VALIDATION_ERROR", "AUTH_ERROR"]
        )
        
        assert policy.max_attempts == 5
        assert policy.backoff == backoff
        assert ValueError in policy.retry_on
        assert "TimeoutError" in policy.retry_on
        assert "VALIDATION_ERROR" in policy.stop_on_codes
    
    def test_should_retry_max_attempts(self) -> None:
        """Test max attempts limit."""
        policy = RetryPolicy(max_attempts=3)
        error = Exception("Test error")
        
        # Should retry within limit
        assert policy.should_retry(1, error) is True
        assert policy.should_retry(2, error) is True
        
        # Should not retry after max attempts
        assert policy.should_retry(3, error) is False
        assert policy.should_retry(4, error) is False
    
    def test_should_retry_stop_codes(self) -> None:
        """Test stopping on specific error codes."""
        policy = RetryPolicy(stop_on_codes=["VALIDATION_ERROR", "AUTH_ERROR"])
        
        # Should not retry validation errors
        validation_error = ValidationError("Invalid input")
        assert policy.should_retry(1, validation_error, "VALIDATION_ERROR") is False
        
        # Should not retry auth errors
        auth_error = Exception("Authentication failed")
        assert policy.should_retry(1, auth_error, "AUTH_ERROR") is False
        
        # Should retry other errors
        other_error = Exception("Network timeout")
        assert policy.should_retry(1, other_error, "NETWORK_ERROR") is True
    
    def test_should_retry_error_types(self) -> None:
        """Test retrying specific error types."""
        policy = RetryPolicy(retry_on=[ValueError, "RuntimeError"])
        
        # Should retry specified error types
        value_error = ValueError("Bad value")
        assert policy.should_retry(1, value_error) is True
        
        runtime_error = RuntimeError("Runtime issue")
        assert policy.should_retry(1, runtime_error) is True
        
        # Should not retry other error types
        type_error = TypeError("Type mismatch")
        assert policy.should_retry(1, type_error) is False
    
    def test_should_retry_task_error_retryable_flag(self) -> None:
        """Test retry behavior with TaskError retryable flag."""
        policy = RetryPolicy()  # Default behavior
        
        # Retryable task error
        retryable_error = TaskError("Temporary failure", retryable=True)
        assert policy.should_retry(1, retryable_error) is True
        
        # Non-retryable task error
        non_retryable_error = TaskError("Permanent failure", retryable=False)
        assert policy.should_retry(1, non_retryable_error) is False
        
        # Non-TaskError should be retried by default when no retry_on is specified
        generic_error = Exception("Generic error")
        assert policy.should_retry(1, generic_error) is True
    
    def test_get_delay_from_backoff(self) -> None:
        """Test delay calculation from backoff strategy."""
        backoff = FixedBackoff(delay=3.5)
        policy = RetryPolicy(backoff=backoff)
        
        assert policy.get_delay(1) == 3.5
        assert policy.get_delay(5) == 3.5
    
    def test_complex_retry_logic(self) -> None:
        """Test complex retry decision logic."""
        policy = RetryPolicy(
            max_attempts=5,
            retry_on=[ConnectionError, "TimeoutError"],
            stop_on_codes=["INVALID_INPUT"]
        )
        
        # Should retry connection errors
        conn_error = ConnectionError("Connection failed")
        assert policy.should_retry(2, conn_error) is True
        
        # Should not retry if stop code is present
        assert policy.should_retry(2, conn_error, "INVALID_INPUT") is False
        
        # Should not retry after max attempts even for retryable errors
        assert policy.should_retry(5, conn_error) is False
        
        # Should not retry non-specified error types
        value_error = ValueError("Bad input")
        assert policy.should_retry(2, value_error) is False