from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, Response
from flask_login import login_required, current_user
from app.models.database import VpsServer, db
from app.utils.ssh_manager import test_ssh_connection, SSHManager
from app.utils.postgres_manager import PostgresManager
from app.routes.auth import first_login_required
import os
import json
import time
import threading
from queue import Queue

servers_bp = Blueprint('servers', __name__, url_prefix='/servers')

# Global dictionary to store progress queues for each server initialization
progress_queues = {}

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
        # Removed user_id assignment for single-user mode
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
                # Initialize server with specified PostgreSQL version
                postgres_version = request.form.get('postgres_version', '17')
                pg_manager = PostgresManager(ssh)
                success, message = pg_manager.initialize_server(postgres_version)
                
                if success:
                    server.initialized = True
                    db.session.commit()
                    flash('Server added successfully and initialized with PostgreSQL and pgBackRest', 'success')
                else:
                    flash(f'Server added successfully but initialization failed: {message}', 'warning')
                
                # Disconnect
                ssh.disconnect()
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
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
    server = VpsServer.query.filter_by(id=id).first_or_404()
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

@servers_bp.route('/initialize-progress/<int:id>')
@login_required
@first_login_required
def initialize_progress(id):
    """Server-Sent Events endpoint for real-time initialization progress"""
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
    def generate_progress():
        # Create a unique queue for this initialization
        queue_key = f"server_{id}_{int(time.time())}"
        progress_queues[queue_key] = Queue()
        
        try:
            # Send initial connection message with queue key
            yield f"data: {json.dumps({'step': 'connecting', 'message': 'Waiting for initialization to start...', 'progress': 5, 'queue_key': queue_key})}\n\n"
            
            # Wait for progress updates from the initialization thread
            while True:
                try:
                    # Wait for progress update with timeout
                    progress_data = progress_queues[queue_key].get(timeout=60)
                    
                    if progress_data is None:  # Sentinel value to end stream
                        break
                        
                    yield f"data: {json.dumps(progress_data)}\n\n"
                    
                    # If this is the final step, break
                    if progress_data.get('step') == 'completed' or progress_data.get('step') == 'error':
                        break
                        
                except:
                    # Timeout or error - send keep-alive
                    yield f"data: {json.dumps({'step': 'keep-alive', 'message': 'Processing...'})}\n\n"
                    
        finally:
            # Clean up the queue
            if queue_key in progress_queues:
                del progress_queues[queue_key]
    
    return Response(generate_progress(), mimetype='text/event-stream')

@servers_bp.route('/initialize/<int:id>', methods=['POST'])
@login_required
@first_login_required
def initialize_server(id):
    """Manual initialization endpoint - now disabled with explicit error."""
    return jsonify({
        'success': False,
        'error': 'INITIALIZATION_DISABLED',
        'message': 'Manual server initialization has been disabled. Please contact your system administrator for server setup.'
    }), 403

@servers_bp.route('/status/<int:id>')
@login_required
@first_login_required
def status(id):
    server = VpsServer.query.filter_by(id=id).first_or_404()
    return render_template('servers/status.html', server=server)

@servers_bp.route('/status-data/<int:id>', methods=['GET'])
@login_required
@first_login_required
def status_data(id):
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
        
        # Get PostgreSQL version
        pg_manager = PostgresManager(ssh)
        postgres_version = pg_manager.get_postgres_version() or "Not Installed"
        
        # Get pgBackRest version
        pgbackrest_version_result = ssh.execute_command("pgbackrest version 2>/dev/null || echo 'Not Installed'")
        pgbackrest_version = pgbackrest_version_result['stdout'].strip() if pgbackrest_version_result['exit_code'] == 0 else "Not Installed"
        
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
            'cpu_usage_percent': cpu_usage_percent,
            'postgres_version': postgres_version,
            'pgbackrest_version': pgbackrest_version
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
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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

@servers_bp.route('/get_postgres_version/<int:id>', methods=['GET'])
@login_required
@first_login_required
def get_postgres_version(id):
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
        
        # Get PostgreSQL version
        pg_manager = PostgresManager(ssh)
        postgres_version = pg_manager.get_postgres_version()
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'version': postgres_version or 'Not installed'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })


@servers_bp.route('/validate-postgres-config/<int:id>', methods=['POST'])
@login_required
@first_login_required
def validate_postgres_config(id):
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
        
        # Check and fix PostgreSQL configuration
        pg_manager = PostgresManager(ssh)
        success, message, changes = pg_manager.check_and_fix_external_connections()
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': success,
            'message': message,
            'changes': changes
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })


@servers_bp.route('/get-postgres-config/<int:id>', methods=['GET'])
@login_required
@first_login_required
def get_postgres_config(id):
    """Get current PostgreSQL configuration."""
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
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
        
        # Initialize PostgreSQL manager
        postgres_manager = PostgresManager(ssh)
        
        # Get current listen_addresses setting
        listen_addresses = postgres_manager.get_postgresql_setting('listen_addresses')
        
        # Get current pg_hba.conf entries
        pg_hba_entries = postgres_manager.get_pg_hba_entries()
        
        # Analyze current configuration
        config = {
            'listen_addresses': listen_addresses,
            'access_type': 'none',
            'allowed_ips': [],
            'auth_method': 'scram-sha-256'
        }
        
        # Determine access type and IPs from pg_hba entries
        if pg_hba_entries:
            # Check if there's a rule for all IPs
            has_all_access = any(
                entry['address'] in ['0.0.0.0/0', '::/0'] 
                for entry in pg_hba_entries
            )
            
            if has_all_access:
                config['access_type'] = 'all'
            else:
                # Collect specific IPs
                specific_ips = []
                for entry in pg_hba_entries:
                    if entry['address'] not in ['0.0.0.0/0', '::/0']:
                        specific_ips.append(entry['address'])
                
                if specific_ips:
                    config['access_type'] = 'specific'
                    config['allowed_ips'] = specific_ips
            
            # Get authentication method from first entry
            if pg_hba_entries:
                config['auth_method'] = pg_hba_entries[0]['method']
        
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'config': config
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error retrieving configuration: {str(e)}'
        })

@servers_bp.route('/configure-postgres-access/<int:id>', methods=['POST'])
@login_required
@first_login_required
def configure_postgres_access(id):
    """Configure PostgreSQL access with advanced options."""
    server = VpsServer.query.filter_by(id=id).first_or_404()
    
    try:
        # Get form data
        data = request.get_json()
        access_type = data.get('access_type', 'all')
        auth_method = data.get('auth_method', 'scram-sha-256')
        allowed_ips = data.get('allowed_ips', '')
        validate_config = data.get('validate_config', True)
        backup_config = data.get('backup_config', True)
        
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
        
        # Create PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        config_manager = pg_manager.config_manager
        
        changes_made = {
            'listen_addresses': False,
            'pg_hba': False,
            'created_files': False,
            'validation_passed': False
        }
        
        # Update listen_addresses in postgresql.conf
        current_listen = config_manager.get_postgresql_setting('listen_addresses')
        if current_listen != '*':
            success, message = config_manager.update_postgresql_setting('listen_addresses', "'*'")
            if success:
                changes_made['listen_addresses'] = True
            else:
                ssh.disconnect()
                return jsonify({
                    'success': False,
                    'message': f'Failed to update listen_addresses: {message}'
                })
        
        # Configure pg_hba.conf
        pg_hba_path = config_manager.find_pg_hba_conf()
        if not pg_hba_path:
            # Try to create it
            data_dir = config_manager.get_data_directory()
            if data_dir:
                pg_hba_path = f"{data_dir}/pg_hba.conf"
                if config_manager.create_default_pg_hba(pg_hba_path):
                    changes_made['created_files'] = True
                else:
                    ssh.disconnect()
                    return jsonify({
                        'success': False,
                        'message': 'Could not create pg_hba.conf'
                    })
            else:
                ssh.disconnect()
                return jsonify({
                    'success': False,
                    'message': 'Could not locate or create pg_hba.conf'
                })
        
        # Backup pg_hba.conf if requested
        if backup_config and pg_hba_path:
            config_manager.system_utils.backup_file(pg_hba_path)
        
        # Remove all existing external access rules (preserve localhost and non-host entries)
        # Create a temporary file with only non-host entries and localhost entries
        temp_file = f"/tmp/pg_hba_temp_{id}"
        ssh.execute_command(f"sudo grep -v '^host' {pg_hba_path} > {temp_file}")
        ssh.execute_command(f"sudo grep '^host.*127\\.0\\.0\\.1' {pg_hba_path} >> {temp_file} || true")
        ssh.execute_command(f"sudo grep '^host.*::1' {pg_hba_path} >> {temp_file} || true")
        ssh.execute_command(f"sudo grep '^host.*localhost' {pg_hba_path} >> {temp_file} || true")
        ssh.execute_command(f"sudo mv {temp_file} {pg_hba_path}")
        ssh.execute_command(f"sudo chown postgres:postgres {pg_hba_path}")
        ssh.execute_command(f"sudo chmod 640 {pg_hba_path}")
        
        # Add new access rules based on configuration
        if access_type == 'all':
            # Allow all IPs
            ipv4_rule = f"echo 'host    all    all    0.0.0.0/0    {auth_method}' | sudo tee -a {pg_hba_path}"
            ipv6_rule = f"echo 'host    all    all    ::/0         {auth_method}' | sudo tee -a {pg_hba_path}"
            
            ipv4_result = ssh.execute_command(ipv4_rule)
            ipv6_result = ssh.execute_command(ipv6_rule)
            
            if ipv4_result['exit_code'] == 0 or ipv6_result['exit_code'] == 0:
                changes_made['pg_hba'] = True
        else:
            # Allow specific IPs
            if allowed_ips:
                ip_list = [ip.strip() for ip in allowed_ips.split('\n') if ip.strip()]
                for ip in ip_list:
                    # Validate IP format (basic validation)
                    if '/' not in ip:
                        ip = f"{ip}/32"  # Add /32 for single IPs
                    
                    rule = f"echo 'host    all    all    {ip.ljust(15)}    {auth_method}' | sudo tee -a {pg_hba_path}"
                    result = ssh.execute_command(rule)
                    
                    if result['exit_code'] == 0:
                        changes_made['pg_hba'] = True
        
        # Validate configuration if requested
        if validate_config:
            # Test PostgreSQL configuration by checking if it can start/reload
            test_result = ssh.execute_command("sudo systemctl reload postgresql")
            if test_result['exit_code'] == 0:
                changes_made['validation_passed'] = True
            else:
                # Try alternative validation method
                alt_test = ssh.execute_command("sudo -u postgres psql -c 'SELECT version();' > /dev/null 2>&1")
                if alt_test['exit_code'] == 0:
                    changes_made['validation_passed'] = True
                else:
                    ssh.disconnect()
                    return jsonify({
                        'success': False,
                        'message': f'Configuration validation failed: PostgreSQL service cannot reload with new configuration'
                    })
        
        # Restart PostgreSQL if changes were made
        if any([changes_made['listen_addresses'], changes_made['pg_hba'], changes_made['created_files']]):
            success, message = config_manager.system_utils.restart_service('postgresql')
            if not success:
                ssh.disconnect()
                return jsonify({
                    'success': False,
                    'message': f'Configuration updated but PostgreSQL restart failed: {message}'
                })
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'message': 'PostgreSQL access configured successfully',
            'changes': changes_made
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })