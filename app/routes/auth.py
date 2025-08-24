from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from app.models.database import User, db
from app.utils.rate_limiter import login_rate_limit, sensitive_operation_limit
from app.utils.session_security import create_secure_session, invalidate_session, require_csrf_token
from functools import wraps
import re
import logging

security_logger = logging.getLogger('security')

auth_bp = Blueprint('auth', __name__)

# Remove custom login_required decorator since we're using Flask-Login's

def first_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if current_user.is_authenticated and current_user.is_first_login:
            flash('You need to change your password', 'warning')
            return redirect(url_for('auth.change_password'))
        return f(*args, **kwargs)
    return decorated_function

@auth_bp.route('/login', methods=['GET', 'POST'])
@login_rate_limit()
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        # Input validation
        if not username or not password:
            security_logger.warning(f"Login attempt with missing credentials from IP {request.remote_addr}")
            flash('Username and password are required', 'danger')
            return render_template('auth/login.html')
        
        # Sanitize username
        if not re.match(r'^[a-zA-Z0-9_.-]+$', username):
            security_logger.warning(f"Login attempt with invalid username format from IP {request.remote_addr}")
            flash('Invalid username format', 'danger')
            return render_template('auth/login.html')
        
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            # Create secure session
            create_secure_session(user)
            login_user(user, remember=True)
            
            security_logger.info(f"Successful login for user {username} from IP {request.remote_addr}")
            
            if user.is_first_login:
                flash('First login detected. Please change your password.', 'warning')
                return redirect(url_for('auth.change_password'))
            
            return redirect(url_for('dashboard.index'))
        else:
            security_logger.warning(f"Failed login attempt for username '{username}' from IP {request.remote_addr}")
            flash('Invalid username or password', 'danger')
    
    return render_template('auth/login.html')

@auth_bp.route('/change-password', methods=['GET', 'POST'])
@login_required
@sensitive_operation_limit()
@require_csrf_token
def change_password():
    user = current_user
    
    if request.method == 'POST':
        current_password = request.form.get('current_password', '')
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        # Enhanced password validation
        validation_errors = validate_password_strength(new_password)
        
        if not user.check_password(current_password):
            security_logger.warning(f"Incorrect current password attempt by user {user.username} from IP {request.remote_addr}")
            flash('Current password is incorrect', 'danger')
        elif new_password != confirm_password:
            flash('New passwords do not match', 'danger')
        elif validation_errors:
            for error in validation_errors:
                flash(error, 'danger')
        else:
            user.set_password(new_password)
            user.is_first_login = False
            db.session.commit()
            
            security_logger.info(f"Password changed successfully for user {user.username} from IP {request.remote_addr}")
            flash('Password changed successfully', 'success')
            return redirect(url_for('dashboard.index'))
    
    return render_template('auth/change_password.html', is_first_login=user.is_first_login)

@auth_bp.route('/logout')
def logout():
    if current_user.is_authenticated:
        security_logger.info(f"User {current_user.username} logged out from IP {request.remote_addr}")
    
    # Secure session invalidation
    invalidate_session()
    logout_user()
    
    flash('You have been logged out', 'info')
    return redirect(url_for('auth.login'))

def validate_password_strength(password):
    """Validate password strength with basic requirements."""
    errors = []
    
    if len(password) < 8:
        errors.append('Password must be at least 8 characters long')
    
    # Basic complexity check - at least one letter and one number
    has_letter = any(c.isalpha() for c in password)
    has_number = any(c.isdigit() for c in password)
    
    if not (has_letter and has_number):
        errors.append('Password must contain at least one letter and one number')
    
    return errors