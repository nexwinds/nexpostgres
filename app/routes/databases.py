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
            username=username,
            password=password,
            vps_server_id=server_id
        )
        
        db.session.add(database)
        db.session.commit()
        
        # Create the database on the server
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
                flash(f'Database record created, but failed to connect to server to create database', 'warning')
                return redirect(url_for('databases.index'))
            
            # Check PostgreSQL installation
            pg_manager = PostgresManager(ssh)
            
            if not pg_manager.check_postgres_installed():
                ssh.disconnect()
                flash(f'Database record created, but PostgreSQL is not installed on the server', 'warning')
                return redirect(url_for('databases.index'))
            
            # Create the database
            success, message = pg_manager.create_database(name, username, password)
            
            # Disconnect
            ssh.disconnect()
            
            if success:
                flash(f'Database added successfully and deployed to server: {message}', 'success')
            else:
                flash(f'Database record created, but deployment failed: {message}', 'warning')
            
        except Exception as e:
            flash(f'Database record created, but deployment failed: {str(e)}', 'warning')
        
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
        
        # Keep track of changes
        password_changed = password and password != database.password
        old_username = database.username
        username_changed = username != old_username
        server_changed = server_id != database.vps_server_id
        
        # Update database record
        database.name = name
        database.username = username
        
        # Only update password if provided
        if password:
            database.password = password
            
        database.vps_server_id = server_id
        
        db.session.commit()
        
        # If password changed, update on server
        if password_changed and not server_changed:
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
                    flash(f'Database record updated, but failed to connect to server to update password', 'warning')
                    return redirect(url_for('databases.index'))
                
                # Check PostgreSQL installation
                pg_manager = PostgresManager(ssh)
                
                if not pg_manager.check_postgres_installed():
                    ssh.disconnect()
                    flash(f'Database record updated, but PostgreSQL is not installed on the server', 'warning')
                    return redirect(url_for('databases.index'))
                
                # Update the user password
                success, message = pg_manager.update_database_user(username, password)
                
                # Disconnect
                ssh.disconnect()
                
                if success:
                    flash(f'Database updated successfully and synchronized with server', 'success')
                else:
                    flash(f'Database record updated, but server synchronization failed: {message}', 'warning')
                
            except Exception as e:
                flash(f'Database record updated, but server synchronization failed: {str(e)}', 'warning')
        else:
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
                'message': 'PostgreSQL is not installed on the server'
            })
        
        # Get PostgreSQL version
        pg_version = pg_manager.get_postgres_version()
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'postgres_version': pg_version
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@databases_bp.route('/credentials/<int:id>')
@login_required
@first_login_required
def credentials(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    # Create the connection URL based on server and database info
    connection_url = f"postgresql://{database.username}:{database.password}@{server.host}:{server.postgres_port}/{database.name}"
    
    return render_template('databases/credentials.html', 
                           database=database, 
                           server=server, 
                           connection_url=connection_url) 