# file: rate_limit_utils.py
import time
import random
import requests
from typing import Optional, Dict, Any, Callable
from functools import wraps

class RateLimiter:
    """
    A rate limiter utility for handling API requests with exponential backoff
    and proper retry logic for rate limiting (HTTP 429) errors.
    """
    
    def __init__(self, 
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 max_retries: int = 10,
                 jitter: bool = True):
        """
        Initialize the rate limiter.
        
        Args:
            base_delay: Starting delay in seconds
            max_delay: Maximum delay in seconds
            max_retries: Maximum number of retries before giving up
            jitter: Whether to add random jitter to delays
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.jitter = jitter
        self.current_delay = base_delay
        self.retry_count = 0
    
    def reset(self):
        """Reset the rate limiter to initial state."""
        self.current_delay = self.base_delay
        self.retry_count = 0
    
    def get_delay(self) -> float:
        """Get the current delay with optional jitter."""
        delay = self.current_delay
        if self.jitter:
            # Add ±25% jitter to avoid thundering herd
            jitter_factor = 0.75 + (random.random() * 0.5)
            delay *= jitter_factor
        return min(delay, self.max_delay)
    
    def wait(self):
        """Wait for the current delay period."""
        delay = self.get_delay()
        print(f"Waiting {delay:.2f} seconds before retry...", end='\r', flush=True)
        time.sleep(delay)
    
    def handle_rate_limit(self, response: requests.Response) -> bool:
        """
        Handle rate limiting response.
        
        Args:
            response: The HTTP response that triggered rate limiting
            
        Returns:
            True if we should retry, False if we should give up
        """
        if response.status_code == 429:
            self.retry_count += 1
            
            if self.retry_count > self.max_retries:
                print(f"Max retries ({self.max_retries}) exceeded. Giving up.")
                return False
            
            # Try to get Retry-After header, fallback to exponential backoff
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    wait_time = int(retry_after)
                    print(f"Rate limited. Server says wait {wait_time} seconds...")
                    time.sleep(wait_time)
                    self.reset()  # Reset delay after following server's instruction
                    return True
                except ValueError:
                    pass
            
            # Exponential backoff with jitter
            print(f"Rate limited (attempt {self.retry_count}/{self.max_retries}). Using exponential backoff...", end='\r', flush=True)
            self.wait()
            self.current_delay = min(self.current_delay * 2, self.max_delay)
            return True
        
        return False
    
    def handle_error(self, error: Exception) -> bool:
        """
        Handle other types of errors that might warrant retrying.
        
        Args:
            error: The exception that occurred
            
        Returns:
            True if we should retry, False if we should give up
        """
        # Check if it's a rate limiting error in the exception message
        if "429" in str(error) or "rate limit" in str(error).lower():
            return self.handle_rate_limit_error()
        
        # For connection errors, we might want to retry
        if isinstance(error, (requests.exceptions.ConnectionError, 
                             requests.exceptions.Timeout,
                             requests.exceptions.RequestException)):
            self.retry_count += 1
            
            if self.retry_count > self.max_retries:
                print(f"Max retries ({self.max_retries}) exceeded for connection error. Giving up.")
                return False
            
            print(f"Connection error (attempt {self.retry_count}/{self.max_retries}). Retrying...")
            self.wait()
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
            return True
        
        return False
    
    def handle_rate_limit_error(self) -> bool:
        """Handle rate limiting when we only have the error, not the response."""
        self.retry_count += 1
        
        if self.retry_count > self.max_retries:
            print(f"Max retries ({self.max_retries}) exceeded. Giving up.")
            return False
        
        print(f"Rate limited (attempt {self.retry_count}/{self.max_retries}). Using exponential backoff...")
        self.wait()
        self.current_delay = min(self.current_delay * 2, self.max_delay)
        return True

def rate_limited_request(func: Callable) -> Callable:
    """
    Decorator to add rate limiting to API request functions.
    
    Usage:
        @rate_limited_request
        def make_api_call():
            # Your API call code here
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        rate_limiter = RateLimiter()
        
        while True:
            try:
                result = func(*args, **kwargs)
                # If we get here, the request was successful
                rate_limiter.reset()
                return result
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    if not rate_limiter.handle_rate_limit(e.response):
                        raise  # Re-raise if we've exceeded max retries
                    continue
                else:
                    raise  # Re-raise non-429 HTTP errors
                    
            except Exception as e:
                if not rate_limiter.handle_error(e):
                    raise  # Re-raise if we shouldn't retry
                continue
    
    return wrapper

def make_rate_limited_request(session: requests.Session, 
                            method: str, 
                            url: str, 
                            rate_limiter: RateLimiter,
                            **kwargs) -> requests.Response:
    """
    Make a rate-limited HTTP request with automatic retry logic.
    
    Args:
        session: Requests session to use
        method: HTTP method (GET, POST, etc.)
        url: URL to request
        rate_limiter: RateLimiter instance
        **kwargs: Additional arguments to pass to requests
        
    Returns:
        The HTTP response
        
    Raises:
        requests.exceptions.RequestException: If all retries are exhausted
    """
    while True:
        try:
            response = session.request(method, url, **kwargs)
            
            # Check for rate limiting
            if response.status_code == 429:
                if not rate_limiter.handle_rate_limit(response):
                    response.raise_for_status()  # This will raise an exception
                continue
            
            # Check for other HTTP errors
            response.raise_for_status()
            
            # Success - reset rate limiter and return response
            rate_limiter.reset()
            return response
            
        except requests.exceptions.RequestException as e:
            if not rate_limiter.handle_error(e):
                raise  # Re-raise if we shouldn't retry
            continue

def create_api_session(api_key: str, 
                      base_delay: float = 1.0,
                      max_delay: float = 60.0,
                      max_retries: int = 10) -> tuple[requests.Session, RateLimiter]:
    """
    Create a requests session with rate limiting for IntakeQ API.
    
    Args:
        api_key: The API key for authentication
        base_delay: Starting delay for rate limiting
        max_delay: Maximum delay for rate limiting
        max_retries: Maximum number of retries
        
    Returns:
        Tuple of (session, rate_limiter)
    """
    session = requests.Session()
    session.headers.update({
        "X-Auth-Key": api_key,
        "User-Agent": "IntakeQ-API-Client/1.0"
    })
    
    rate_limiter = RateLimiter(
        base_delay=base_delay,
        max_delay=max_delay,
        max_retries=max_retries
    )
    
    return session, rate_limiter
