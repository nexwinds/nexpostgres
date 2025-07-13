from flask import Blueprint, render_template
from flask_login import login_required
from app.routes.auth import first_login_required

pages_bp = Blueprint('pages', __name__)

@pages_bp.route('/pages')
@login_required
@first_login_required
def index():
    """Display pages management."""
    return render_template('pages/index.html')

@pages_bp.route('/pages/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    """Add a new page."""
    return render_template('pages/add.html')

@pages_bp.route('/pages/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    """Edit an existing page."""
    return render_template('pages/edit.html', page_id=id)