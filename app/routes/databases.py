from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import VpsServer, PostgresDatabase, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.routes.auth import login_required, first_login_required

databases_bp = Blueprint('databases', __name__, url_prefix='/databases')

@databases_bp.route('/')
@login_required
@first_login_required
def index():
    databases = PostgresDatabase.query.all()
    return render_template('databases/index.html', databases=databases)

@databases_bp.route('/server/<int:server_id>')
@login_required
@first_login_required
def by_server(server_id):
    server = VpsServer.query.get_or_404(server_id)
    databases = PostgresDatabase.query.filter_by(vps_server_id=server_id).all()
    return render_template('databases/by_server.html', server=server, databases=databases)

@databases_bp.route('/sync/<int:server_id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def sync_databases(server_id):
    server = VpsServer.query.get_or_404(server_id)
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('databases.by_server', server_id=server_id))
        
        # Check PostgreSQL installation
        pg_manager = PostgresManager(ssh)
        
        if not pg_manager.check_postgres_installed():
            ssh.disconnect()
            flash('PostgreSQL is not installed on the server', 'danger')
            return redirect(url_for('databases.by_server', server_id=server_id))
        
        # Get list of databases from server
        server_databases = pg_manager.list_databases()
        
        # Disconnect SSH
        ssh.disconnect()
        
        # Get list of databases already in the app for this server
        app_databases = PostgresDatabase.query.filter_by(vps_server_id=server_id).all()
        app_database_names = [db.name for db in app_databases]
        
        # Find databases that are on the server but not in the app
        new_databases = [db for db in server_databases if db['name'] not in app_database_names]
        
        if request.method == 'POST':
            # Get selected databases to import
            selected_databases = request.form.getlist('selected_databases')
            
            # Import selected databases
            for db_name in selected_databases:
                # Find the database in the server_databases list
                db_info = next((db for db in server_databases if db['name'] == db_name), None)
                
                if db_info:
                    # Create database record
                    database = PostgresDatabase(
                        name=db_name,
                        port=5432,  # Default PostgreSQL port
                        username=db_info['owner'],
                        password='',  # Empty password, user will need to update
                        vps_server_id=server_id
                    )
                    
                    db.session.add(database)
            
            db.session.commit()
            
            flash(f'Successfully imported {len(selected_databases)} database(s)', 'success')
            return redirect(url_for('databases.by_server', server_id=server_id))
        
        return render_template('databases/sync.html', server=server, new_databases=new_databases)
        
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
        return redirect(url_for('databases.by_server', server_id=server_id))

@databases_bp.route('/sync-all', methods=['POST'])
@login_required
@first_login_required
def sync_all_servers():
    try:
        servers = VpsServer.query.all()
        total_imported = 0
        
        for server in servers:
            # Connect to server via SSH
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_path=server.ssh_key_path,
                ssh_key_content=server.ssh_key_content
            )
            
            if not ssh.connect():
                continue
            
            # Check PostgreSQL installation
            pg_manager = PostgresManager(ssh)
            
            if not pg_manager.check_postgres_installed():
                ssh.disconnect()
                continue
            
            # Get list of databases from server
            server_databases = pg_manager.list_databases()
            
            # Disconnect SSH
            ssh.disconnect()
            
            # Get list of databases already in the app for this server
            app_databases = PostgresDatabase.query.filter_by(vps_server_id=server.id).all()
            app_database_names = [db.name for db in app_databases]
            
            # Find databases that are on the server but not in the app
            new_databases = [db for db in server_databases if db['name'] not in app_database_names]
            
            # Import all new databases
            for db_info in new_databases:
                # Create database record
                database = PostgresDatabase(
                    name=db_info['name'],
                    port=5432,  # Default PostgreSQL port
                    username=db_info['owner'],
                    password='',  # Empty password, user will need to update
                    vps_server_id=server.id
                )
                
                db.session.add(database)
                total_imported += 1
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Successfully imported {total_imported} database(s)',
            'imported_count': total_imported
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@databases_bp.route('/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    servers = VpsServer.query.all()
    
    if not servers:
        flash('You need to add a server first', 'warning')
        return redirect(url_for('servers.add'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        port = request.form.get('port', 5432, type=int)
        username = request.form.get('username')
        password = request.form.get('password')
        server_id = request.form.get('server_id', type=int)
        
        # Validate data
        server = VpsServer.query.get(server_id)
        if not server:
            flash('Selected server does not exist', 'danger')
            return render_template('databases/add.html', servers=servers)
        
        # Check if database already exists on server
        existing = PostgresDatabase.query.filter_by(name=name, vps_server_id=server_id).first()
        if existing:
            flash(f'Database "{name}" already exists on this server', 'danger')
            return render_template('databases/add.html', servers=servers)
        
        # Create database record
        database = PostgresDatabase(
            name=name,
            port=port,
            username=username,
            password=password,
            vps_server_id=server_id
        )
        
        db.session.add(database)
        db.session.commit()
        
        flash('Database added successfully', 'success')
        return redirect(url_for('databases.index'))
    
    return render_template('databases/add.html', servers=servers)

@databases_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    database = PostgresDatabase.query.get_or_404(id)
    servers = VpsServer.query.all()
    
    if request.method == 'POST':
        name = request.form.get('name')
        port = request.form.get('port', 5432, type=int)
        username = request.form.get('username')
        password = request.form.get('password')
        server_id = request.form.get('server_id', type=int)
        
        # Validate data
        server = VpsServer.query.get(server_id)
        if not server:
            flash('Selected server does not exist', 'danger')
            return render_template('databases/edit.html', database=database, servers=servers)
        
        # Check if database already exists on server (if name or server changed)
        if (name != database.name or server_id != database.vps_server_id):
            existing = PostgresDatabase.query.filter_by(name=name, vps_server_id=server_id).first()
            if existing:
                flash(f'Database "{name}" already exists on this server', 'danger')
                return render_template('databases/edit.html', database=database, servers=servers)
        
        # Update database record
        database.name = name
        database.port = port
        database.username = username
        
        # Only update password if provided
        if password:
            database.password = password
            
        database.vps_server_id = server_id
        
        db.session.commit()
        
        flash('Database updated successfully', 'success')
        return redirect(url_for('databases.index'))
    
    return render_template('databases/edit.html', database=database, servers=servers)

@databases_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
@first_login_required
def delete(id):
    database = PostgresDatabase.query.get_or_404(id)
    
    db.session.delete(database)
    db.session.commit()
    
    flash('Database deleted successfully', 'success')
    return redirect(url_for('databases.index'))

@databases_bp.route('/check/<int:id>')
@login_required
@first_login_required
def check(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Check PostgreSQL installation
        pg_manager = PostgresManager(ssh)
        
        if not pg_manager.check_postgres_installed():
            ssh.disconnect()
            return jsonify({
                'success': False,
                'message': 'PostgreSQL is not installed on the server',
                'can_install': True
            })
        
        # Get PostgreSQL version
        pg_version = pg_manager.get_postgres_version()
        
        # Check pgBackRest installation
        pgbackrest_installed = pg_manager.check_pgbackrest_installed()
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'postgres_version': pg_version,
            'pgbackrest_installed': pgbackrest_installed
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@databases_bp.route('/install-postgres/<int:id>', methods=['POST'])
@login_required
@first_login_required
def install_postgres(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Install PostgreSQL
        pg_manager = PostgresManager(ssh)
        success = pg_manager.install_postgres()
        
        # Disconnect
        ssh.disconnect()
        
        if success:
            return jsonify({
                'success': True,
                'message': 'PostgreSQL installed successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to install PostgreSQL'
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@databases_bp.route('/install-pgbackrest/<int:id>', methods=['POST'])
@login_required
@first_login_required
def install_pgbackrest(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Install pgBackRest
        pg_manager = PostgresManager(ssh)
        success = pg_manager.install_pgbackrest()
        
        # Disconnect
        ssh.disconnect()
        
        if success:
            return jsonify({
                'success': True,
                'message': 'pgBackRest installed successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to install pgBackRest'
            })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }) 