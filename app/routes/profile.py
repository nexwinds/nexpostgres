from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from app.models.database import User, db
from werkzeug.security import generate_password_hash
from app.routes.auth import first_login_required

profile_bp = Blueprint('profile', __name__)

@profile_bp.route('/profile')
@login_required
@first_login_required
def index():
    """Display user profile."""
    return render_template('profile/index.html', user=current_user)

@profile_bp.route('/profile/edit', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit():
    """Edit user profile."""
    if request.method == 'POST':
        # Update user profile information
        current_user.username = request.form.get('username', current_user.username)
        
        # Update password if provided
        new_password = request.form.get('new_password')
        if new_password:
            current_user.password_hash = generate_password_hash(new_password)
            current_user.is_first_login = False
        
        try:
            db.session.commit()
            flash('Profile updated successfully', 'success')
            return redirect(url_for('profile.index'))
        except Exception as e:
            db.session.rollback()
            flash('Error updating profile', 'error')
    
    return render_template('profile/edit.html', user=current_user)

@profile_bp.route('/profile/settings')
@login_required
@first_login_required
def settings():
    """Display user settings."""
    return render_template('profile/settings.html', user=current_user)