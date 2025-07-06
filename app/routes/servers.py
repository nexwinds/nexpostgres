from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import VpsServer, db
from app.utils.ssh_manager import test_ssh_connection, SSHManager
from app.utils.postgres_manager import PostgresManager
from app.routes.auth import login_required, first_login_required
import os
import json

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
    server = VpsServer(port=22, postgres_port=5432)
    
    if request.method == 'POST':
        server.name = request.form.get('name')
        server.host = request.form.get('host')
        server.port = request.form.get('port', 22, type=int)
        server.postgres_port = request.form.get('postgres_port', 5432, type=int)
        server.username = request.form.get('username')
        server.ssh_key_content = request.form.get('ssh_key_content')
        server.initialized = False  # Set initial status as not initialized
        
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
        
        # Initialize the server in the background
        try:
            # Create SSH connection
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if ssh.connect():
                # Initialize server
                pg_manager = PostgresManager(ssh)
                success, message = pg_manager.initialize_server()
                
                # Disconnect
                ssh.disconnect()
                
                if success:
                    server.initialized = True
                    db.session.commit()
                    flash('Server added successfully and initialized with PostgreSQL and pgBackRest', 'success')
                else:
                    flash(f'Server added successfully but initialization failed: {message}', 'warning')
            else:
                flash('Server added successfully but initialization could not start', 'warning')
        except Exception as e:
            flash(f'Server added successfully but initialization failed: {str(e)}', 'warning')
        
        return redirect(url_for('servers.index'))
    
    return render_template('servers/add.html', server=server)

@servers_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    server = VpsServer.query.get_or_404(id)
    
    # No need to load SSH key content from file anymore
    # as we no longer store SSH keys on disk
    
    if request.method == 'POST':
        server.name = request.form.get('name')
        server.host = request.form.get('host')
        server.port = request.form.get('port', 22, type=int)
        server.postgres_port = request.form.get('postgres_port', 5432, type=int)
        server.username = request.form.get('username')
        server.ssh_key_content = request.form.get('ssh_key_content')
        
        if not server.ssh_key_content.strip():
            flash('SSH key content cannot be empty', 'danger')
            return render_template('servers/edit.html', server=server)
        
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

@servers_bp.route('/initialize/<int:id>', methods=['POST'])
@login_required
@first_login_required
def initialize_server(id):
    server = VpsServer.query.get_or_404(id)
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Initialize server
        pg_manager = PostgresManager(ssh)
        success, message = pg_manager.initialize_server()
        
        # Disconnect
        ssh.disconnect()
        
        if success:
            # Update server status in database
            server.initialized = True
            db.session.commit()
            
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            return jsonify({
                'success': False,
                'message': message
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@servers_bp.route('/status/<int:id>')
@login_required
@first_login_required
def status(id):
    server = VpsServer.query.get_or_404(id)
    return render_template('servers/status.html', server=server)

@servers_bp.route('/status-data/<int:id>', methods=['GET'])
@login_required
@first_login_required
def status_data(id):
    server = VpsServer.query.get_or_404(id)
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Get system information
        os_info = ssh.execute_command("cat /etc/os-release | grep -E '^(NAME|VERSION)' | tr '\\n' ' '")
        os_name = os_info['stdout'].strip()
        
        # Get public IPv4 address
        ip_info = ssh.execute_command("curl -s https://ipinfo.io/ip || hostname -I | awk '{print $1}'")
        public_ip = ip_info['stdout'].strip()
        
        # Get CPU info
        cpu_info = ssh.execute_command("nproc --all")
        vcpu_count = cpu_info['stdout'].strip()
        
        # Get memory info
        mem_info = ssh.execute_command("free -m | grep Mem")
        mem_parts = mem_info['stdout'].strip().split()
        total_ram = int(mem_parts[1])
        used_ram = int(mem_parts[2])
        ram_usage_percent = round((used_ram / total_ram) * 100, 2)
        
        # Get disk info
        disk_info = ssh.execute_command("df -h / | tail -1")
        disk_parts = disk_info['stdout'].strip().split()
        total_disk = disk_parts[1]
        used_disk = disk_parts[2]
        free_disk = disk_parts[3]
        disk_usage_percent = disk_parts[4].replace('%', '')
        
        # Get current CPU usage
        cpu_usage = ssh.execute_command("top -bn1 | grep 'Cpu(s)' | awk '{print $2}'")
        cpu_usage_percent = cpu_usage['stdout'].strip()
        
        # Disconnect
        ssh.disconnect()
        
        # Prepare response data
        system_info = {
            'success': True,
            'os': os_name,
            'ip': public_ip,
            'vcpu': vcpu_count,
            'ram': {
                'total': total_ram,
                'used': used_ram,
                'usage_percent': ram_usage_percent
            },
            'disk': {
                'total': total_disk,
                'used': used_disk,
                'free': free_disk,
                'usage_percent': disk_usage_percent
            },
            'cpu_usage_percent': cpu_usage_percent
        }
        
        return jsonify(system_info)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@servers_bp.route('/restart/<int:id>', methods=['POST'])
@login_required
@first_login_required
def restart_server(id):
    server = VpsServer.query.get_or_404(id)
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Issue reboot command
        ssh.execute_command("sudo reboot")
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'message': 'Server restart initiated successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@servers_bp.route('/update/<int:id>', methods=['POST'])
@login_required
@first_login_required
def update_server(id):
    server = VpsServer.query.get_or_404(id)
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Run system update command
        update_result = ssh.execute_command("sudo apt-get update && sudo apt-get upgrade -y")
        
        # Disconnect
        ssh.disconnect()
        
        if update_result['exit_code'] == 0:
            return jsonify({
                'success': True,
                'message': 'Server updated successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': f"Update failed: {update_result['stderr']}"
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }) 