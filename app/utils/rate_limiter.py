from flask import request, current_app
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from functools import wraps

logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["1000 per hour"]
)

def init_rate_limiter(app):
    """Initialize rate limiter with Flask app."""
    # Configure storage URI if provided
    storage_uri = app.config.get('RATELIMIT_STORAGE_URL')
    if storage_uri:
        limiter.storage_uri = storage_uri
    
    limiter.init_app(app)
    return limiter

def rate_limit_exceeded_handler(e):
    """Custom handler for rate limit exceeded errors."""
    logger.warning(f"Rate limit exceeded for IP {request.remote_addr}: {str(e)}")
    
    if request.is_json:
        from flask import jsonify
        return jsonify({
            'error': 'Rate limit exceeded',
            'message': 'Too many requests. Please try again later.',
            'retry_after': e.retry_after
        }), 429
    
    from flask import render_template
    return render_template('errors/rate_limit.html', retry_after=e.retry_after), 429

def login_rate_limit():
    """Decorator for login rate limiting."""
    def decorator(f):
        @wraps(f)
        @limiter.limit(lambda: current_app.config.get('LOGIN_RATE_LIMIT', '5 per minute'))
        def decorated_function(*args, **kwargs):
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def api_rate_limit(limit="100 per hour"):
    """Decorator for API endpoint rate limiting."""
    def decorator(f):
        @wraps(f)
        @limiter.limit(limit)
        def decorated_function(*args, **kwargs):
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def sensitive_operation_limit(limit="10 per minute"):
    """Decorator for sensitive operations like backup creation, server addition."""
    def decorator(f):
        @wraps(f)
        @limiter.limit(limit)
        def decorated_function(*args, **kwargs):
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def get_rate_limit_status(endpoint=None):
    """Get current rate limit status for debugging."""
    try:
        if endpoint:
            return limiter.get_window_stats(endpoint)
        return {
            'current_requests': limiter.current_requests,
            'remaining_requests': limiter.remaining_requests
        }
    except Exception as e:
        logger.error(f"Error getting rate limit status: {str(e)}")
        return None

def reset_rate_limit(key=None):
    """Reset rate limit for a specific key (admin function)."""
    try:
        if key:
            limiter.reset(key)
        else:
            limiter.reset()
        logger.info(f"Rate limit reset for key: {key or 'all'}")
        return True
    except Exception as e:
        logger.error(f"Error resetting rate limit: {str(e)}")
        return False