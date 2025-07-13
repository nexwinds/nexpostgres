from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from app.models.database import User, db
from functools import wraps

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
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            login_user(user, remember=True)
            
            if user.is_first_login:
                flash('First login detected. Please change your password.', 'warning')
                return redirect(url_for('auth.change_password'))
            
            return redirect(url_for('dashboard.index'))
        
        flash('Invalid username or password', 'danger')
    
    return render_template('auth/login.html')

@auth_bp.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    user = current_user
    
    if request.method == 'POST':
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        
        if not user.check_password(current_password):
            flash('Current password is incorrect', 'danger')
        elif new_password != confirm_password:
            flash('New passwords do not match', 'danger')
        elif len(new_password) < 8:
            flash('New password must be at least 8 characters long', 'danger')
        else:
            user.set_password(new_password)
            user.is_first_login = False
            db.session.commit()
            
            flash('Password changed successfully', 'success')
            return redirect(url_for('dashboard.index'))
    
    return render_template('auth/change_password.html', is_first_login=user.is_first_login)

@auth_bp.route('/logout')
def logout():
    logout_user()
    flash('You have been logged out', 'info')
    return redirect(url_for('auth.login'))