from flask import session, request, current_app, jsonify, render_template, redirect, url_for, flash, abort
from flask_wtf.csrf import CSRFProtect, validate_csrf
from flask_login import current_user
from datetime import datetime, timedelta
import secrets
import logging
from functools import wraps

logger = logging.getLogger(__name__)

# Initialize CSRF protection
csrf = CSRFProtect()

def init_session_security(app):
    """Initialize session security with Flask app."""
    csrf.init_app(app)
    
    # Register CSRF error handler
    def csrf_error(reason):
        logger.warning(f"CSRF validation failed: {reason} for IP {request.remote_addr}")
        
        if request.is_json:
            return jsonify({
                'error': 'CSRF token validation failed',
                'message': 'Invalid or missing CSRF token. Please refresh the page and try again.'
            }), 400
        
        return render_template('errors/csrf.html', reason=reason), 400
    
    # Register the CSRF error handler with the app
    app.errorhandler(400)(csrf_error)
    
    # Add session security hooks
    app.before_request(validate_session_security)
    app.after_request(update_session_activity)
    
    return csrf

def validate_session_security():
    """Validate session security before each request."""
    # Skip validation for static files and auth endpoints
    if request.endpoint and (request.endpoint.startswith('static') or 
                           request.endpoint.startswith('auth.login')):
        return
    
    # Check for session hijacking
    if current_user.is_authenticated:
        if not validate_session_fingerprint():
            logger.warning(f"Potential session hijacking detected for user {current_user.username} from IP {request.remote_addr}")
            invalidate_session()
            flash('Your session has been invalidated for security reasons. Please log in again.', 'warning')
            return redirect(url_for('auth.login'))
        
        # Check session timeout
        if is_session_expired():
            logger.info(f"Session expired for user {current_user.username}")
            invalidate_session()
            flash('Your session has expired. Please log in again.', 'info')
            return redirect(url_for('auth.login'))

def validate_session_fingerprint():
    """Validate session fingerprint to detect session hijacking."""
    current_fingerprint = generate_session_fingerprint()
    stored_fingerprint = session.get('_fingerprint')
    
    if not stored_fingerprint:
        # First time, store the fingerprint
        session['_fingerprint'] = current_fingerprint
        return True
    
    return current_fingerprint == stored_fingerprint

def generate_session_fingerprint():
    """Generate a session fingerprint based on user agent and IP."""
    import hashlib
    
    user_agent = request.headers.get('User-Agent', '')
    ip_address = request.remote_addr or ''
    
    # Create a hash of user agent and IP
    fingerprint_data = f"{user_agent}:{ip_address}"
    return hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]

def is_session_expired():
    """Check if the current session has expired."""
    last_activity = session.get('_last_activity')
    if not last_activity:
        return False
    
    session_lifetime = current_app.config.get('PERMANENT_SESSION_LIFETIME', timedelta(hours=24))
    if isinstance(session_lifetime, int):
        session_lifetime = timedelta(seconds=session_lifetime)
    
    return datetime.utcnow() - last_activity > session_lifetime

def update_session_activity(response):
    """Update session activity timestamp."""
    if current_user.is_authenticated:
        session['_last_activity'] = datetime.utcnow()
        session.permanent = True
    
    return response

def invalidate_session():
    """Securely invalidate the current session."""
    from flask_login import logout_user
    
    # Clear all session data
    session.clear()
    
    # Logout the user
    logout_user()
    
    # Regenerate session ID
    session.regenerate()
    
    logger.info(f"Session invalidated for IP {request.remote_addr}")

def invalidate_all_user_sessions(user_id):
    """Invalidate all sessions for a specific user (admin function)."""
    # This would require a more sophisticated session store
    # For now, we'll log the action
    logger.info(f"Request to invalidate all sessions for user ID {user_id}")
    # TODO: Implement with Redis or database-backed session store

def create_secure_session(user):
    """Create a secure session for the user."""
    # Generate new session ID
    session.regenerate()
    
    # Set session data
    session['_user_id'] = user.id
    session['_login_time'] = datetime.utcnow()
    session['_last_activity'] = datetime.utcnow()
    session['_fingerprint'] = generate_session_fingerprint()
    session['_csrf_token'] = secrets.token_hex(16)
    
    # Make session permanent
    session.permanent = True
    
    logger.info(f"Secure session created for user {user.username} from IP {request.remote_addr}")

def require_csrf_token(f):
    """Decorator to require CSRF token validation for specific endpoints."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method in ['POST', 'PUT', 'DELETE', 'PATCH']:
            try:
                validate_csrf(request.headers.get('X-CSRFToken') or 
                            request.form.get('csrf_token'))
            except Exception as e:
                logger.warning(f"CSRF validation failed: {str(e)} for IP {request.remote_addr}")
                
                if request.is_json:
                    return jsonify({
                        'error': 'CSRF token validation failed',
                        'message': 'Invalid or missing CSRF token'
                    }), 400
                
                abort(400)
        
        return f(*args, **kwargs)
    return decorated_function

def get_csrf_token():
    """Get CSRF token for the current session."""
    return session.get('_csrf_token', csrf.generate_csrf())

def validate_session_integrity():
    """Validate overall session integrity."""
    checks = {
        'fingerprint_valid': validate_session_fingerprint(),
        'not_expired': not is_session_expired(),
        'has_csrf_token': '_csrf_token' in session,
        'user_authenticated': current_user.is_authenticated
    }
    
    return all(checks.values()), checks