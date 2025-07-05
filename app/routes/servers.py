from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import VpsServer, db
from app.utils.ssh_manager import test_ssh_connection
from app.routes.auth import login_required, first_login_required
import os

servers_bp = Blueprint('servers', __name__, url_prefix='/servers')

@servers_bp.route('/')
@login_required
@first_login_required
def index():
    servers = VpsServer.query.all()
    return render_template('servers/index.html', servers=servers)

@servers_bp.route('/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    server = VpsServer(port=22)
    
    if request.method == 'POST':
        server.name = request.form.get('name')
        server.host = request.form.get('host')
        server.port = request.form.get('port', 22, type=int)
        server.username = request.form.get('username')
        server.ssh_key_content = request.form.get('ssh_key_content')
        
        if not server.ssh_key_content.strip():
            flash('SSH key content cannot be empty', 'danger')
            return render_template('servers/add.html', server=server)
        
        # Test SSH connection
        if request.form.get('test_connection') == 'yes' or True:  # Always test on add
            connection_ok = test_ssh_connection(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if not connection_ok:
                flash('Failed to connect to the server. Please check your settings.', 'danger')
                return render_template('servers/add.html', server=server)
        
        # Create server record
        db.session.add(server)
        db.session.commit()
        
        flash('Server added successfully', 'success')
        return redirect(url_for('servers.index'))
    
    return render_template('servers/add.html', server=server)

@servers_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    server = VpsServer.query.get_or_404(id)
    
    # Load SSH key content if available
    if server.ssh_key_path and not server.ssh_key_content:
        try:
            if os.path.isfile(server.ssh_key_path):
                with open(server.ssh_key_path, 'r') as f:
                    server.ssh_key_content = f.read()
        except Exception:
            pass
    
    if request.method == 'POST':
        server.name = request.form.get('name')
        server.host = request.form.get('host')
        server.port = request.form.get('port', 22, type=int)
        server.username = request.form.get('username')
        server.ssh_key_content = request.form.get('ssh_key_content')
        
        if not server.ssh_key_content.strip():
            flash('SSH key content cannot be empty', 'danger')
            return render_template('servers/edit.html', server=server)
            
        server.ssh_key_path = None
            
        # Only test connection if requested
        if request.form.get('test_connection') == 'yes':
            connection_ok = test_ssh_connection(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if not connection_ok:
                flash('Failed to connect to the server. Please check your settings.', 'danger')
                return render_template('servers/edit.html', server=server)
        
        db.session.commit()
        flash('Server updated successfully', 'success')
        return redirect(url_for('servers.index'))
    
    return render_template('servers/edit.html', server=server)

@servers_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
@first_login_required
def delete(id):
    server = VpsServer.query.get_or_404(id)
    db.session.delete(server)
    db.session.commit()
    flash('Server deleted successfully', 'success')
    return redirect(url_for('servers.index'))

@servers_bp.route('/test-connection', methods=['POST'])
@login_required
@first_login_required
def test_connection():
    host = request.form.get('host')
    port = request.form.get('port', 22, type=int)
    username = request.form.get('username')
    ssh_key_content = request.form.get('ssh_key_content')
    
    if not ssh_key_content.strip():
        return jsonify({'success': False, 'message': 'SSH key content cannot be empty'})
    
    connection_ok = test_ssh_connection(
        host=host,
        port=port,
        username=username,
        ssh_key_content=ssh_key_content
    )
    
    if connection_ok:
        return jsonify({'success': True, 'message': 'Connection successful'})
    else:
        return jsonify({'success': False, 'message': 'Connection failed'}) 