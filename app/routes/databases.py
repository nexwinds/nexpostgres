from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app.models.database import VpsServer, PostgresDatabase, PostgresDatabaseUser, RestoreLog, db
from app.utils.database_service import DatabaseService, DatabaseImportService
from app.utils.validation_service import ValidationService
import secrets
import string
from datetime import datetime

databases_bp = Blueprint('databases', __name__)


@databases_bp.route('/databases')
@login_required
def databases():
    """Display all databases for the current user."""
    user_databases = PostgresDatabase.query.join(VpsServer).filter(
        # Removed user_id filtering for single-user mode
    ).all()
    return render_template('databases.html', databases=user_databases)


@databases_bp.route('/databases/add', methods=['GET', 'POST'])
@login_required
def add_database():
    """Add a new database."""
    if request.method == 'GET':
        servers = VpsServer.query.all()
        return render_template('add_database.html', servers=servers)
    
    # POST request - process form
    data = request.form.to_dict()
    
    # Validate required fields
    required_fields = ['name', 'vps_server_id']
    field_errors = ValidationService.validate_required_fields(data, required_fields)
    if field_errors:
        for error in field_errors:
            flash(error, 'error')
        return redirect(url_for('databases.add_database'))
    
    # Validate database name
    name_valid, name_error = ValidationService.validate_database_name(data['name'])
    if not name_valid:
        flash(name_error, 'error')
        return redirect(url_for('databases.add_database'))
    
    # Check if database already exists
    if DatabaseService.validate_database_exists(data['name'], int(data['vps_server_id'])):
        flash('A database with this name already exists on the selected server', 'error')
        return redirect(url_for('databases.add_database'))
    
    # Get server and validate ownership
    server = VpsServer.query.filter_by(
        id=data['vps_server_id'], 
        # Removed user_id for single-user mode
    ).first()
    
    if not server:
        flash('Invalid server selected', 'error')
        return redirect(url_for('databases.add_database'))
    
    # Generate username and password
    existing_users = [user.username for user in PostgresDatabaseUser.query.all()]
    username = ValidationService.generate_username(data['name'], existing_users)
    password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))
    
    # Create database on server
    success, message = DatabaseService.execute_with_postgres(
        server, 
        'Database creation',
        DatabaseService.create_database_operation,
        data['name'], username, password
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.add_database'))
    
    # Save to database
    try:
        new_database = PostgresDatabase(
            name=data['name'],
            vps_server_id=server.id
        )
        db.session.add(new_database)
        db.session.flush()  # Get the ID
        
        new_user = PostgresDatabaseUser(
            username=username,
            password=password,
            database_id=new_database.id,
            permission_level='admin',
            is_primary=True
        )
        db.session.add(new_user)
        db.session.commit()
        
        flash('Database created successfully', 'success')
        return redirect(url_for('databases.databases'))
        
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to save database information: {str(e)}', 'error')
        return redirect(url_for('databases.add_database'))


@databases_bp.route('/databases/<int:database_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_database(database_id):
    """Edit database credentials."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    if request.method == 'GET':
        primary_user = PostgresDatabaseUser.query.filter_by(
            database_id=database.id, 
            is_primary=True
        ).first()
        
        current_permission = DatabaseService.get_current_user_permission(
            database.vps_server, database.name, primary_user.username if primary_user else ''
        )
        
        return render_template('edit_database.html', 
                             database=database, 
                             primary_user=primary_user,
                             current_permission=current_permission)
    
    # POST request - update password
    new_password = request.form.get('new_password', '').strip()
    
    # Validate password
    password_valid, password_error = ValidationService.validate_password(new_password)
    if not password_valid:
        flash(password_error, 'error')
        return redirect(url_for('databases.edit_database', database_id=database_id))
    
    primary_user = PostgresDatabaseUser.query.filter_by(
        database_id=database.id, 
        is_primary=True
    ).first()
    
    if not primary_user:
        flash('Primary user not found', 'error')
        return redirect(url_for('databases.edit_database', database_id=database_id))
    
    # Update password on server
    success, message = DatabaseService.execute_with_postgres(
        database.vps_server,
        'Password update',
        DatabaseService.update_user_password_operation,
        primary_user.username, new_password
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.edit_database', database_id=database_id))
    
    # Update in database
    try:
        primary_user.password = new_password
        db.session.commit()
        flash('Password updated successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to update password in database: {str(e)}', 'error')
    
    return redirect(url_for('databases.edit_database', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/users/add', methods=['GET', 'POST'])
@login_required
def add_database_user(database_id):
    """Add a new user to a database."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    if request.method == 'GET':
        return render_template('add_database_user.html', database=database)
    
    # POST request - process form
    data = request.form.to_dict()
    
    # Validate required fields
    required_fields = ['username', 'password', 'permission_level']
    field_errors = ValidationService.validate_required_fields(data, required_fields)
    
    # Validate individual fields
    validations = [
        ValidationService.validate_username(data.get('username', '')),
        ValidationService.validate_password(data.get('password', '')),
        ValidationService.validate_permission_level(data.get('permission_level', ''))
    ]
    
    if field_errors:
        for error in field_errors:
            flash(error, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    if not ValidationService.validate_and_flash_errors(validations):
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Check if user already exists
    if DatabaseService.validate_user_exists(data['username'], database_id):
        flash('A user with this username already exists for this database', 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Create user on server
    success, message = DatabaseService.execute_with_postgres(
        database.vps_server,
        'User creation',
        DatabaseService.create_user_operation,
        data['username'], data['password'], database.name, data['permission_level']
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Save to database
    try:
        new_user = PostgresDatabaseUser(
            username=data['username'],
            password=data['password'],
            database_id=database_id,
            permission_level=data['permission_level'],
            is_primary=False
        )
        db.session.add(new_user)
        db.session.commit()
        
        flash('User added successfully', 'success')
        return redirect(url_for('databases.database_users', database_id=database_id))
        
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to save user information: {str(e)}', 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/users')
@login_required
def database_users(database_id):
    """Display users for a specific database."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    users = PostgresDatabaseUser.query.filter_by(database_id=database_id).all()
    user_permissions = DatabaseService.get_user_permissions(database.vps_server, database.name)
    
    return render_template('database_users.html', 
                         database=database, 
                         users=users, 
                         user_permissions=user_permissions)


@databases_bp.route('/databases/<int:database_id>/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_database_user(database_id, user_id):
    """Edit database user permissions."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    user = PostgresDatabaseUser.query.filter_by(
        id=user_id, 
        database_id=database_id
    ).first_or_404()
    
    if request.method == 'GET':
        current_permission = DatabaseService.get_current_user_permission(
            database.vps_server, database.name, user.username
        )
        return render_template('edit_database_user.html', 
                             database=database, 
                             user=user, 
                             current_permission=current_permission)
    
    # POST request - update permissions
    new_permission = request.form.get('permission_level', '').strip()
    
    # Validate permission level
    permission_valid, permission_error = ValidationService.validate_permission_level(new_permission)
    if not permission_valid:
        flash(permission_error, 'error')
        return redirect(url_for('databases.edit_database_user', 
                              database_id=database_id, user_id=user_id))
    
    # Update permissions on server
    success, message = DatabaseService.execute_with_postgres(
        database.vps_server,
        'Permission update',
        DatabaseService.create_user_operation,  # This handles permission updates too
        user.username, user.password, database.name, new_permission
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.edit_database_user', 
                              database_id=database_id, user_id=user_id))
    
    # Update in database
    try:
        user.permission_level = new_permission
        db.session.commit()
        flash('Permissions updated successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to update permissions in database: {str(e)}', 'error')
    
    return redirect(url_for('databases.edit_database_user', 
                          database_id=database_id, user_id=user_id))


@databases_bp.route('/databases/<int:database_id>/users/<int:user_id>/delete', methods=['POST'])
@login_required
def delete_database_user(database_id, user_id):
    """Delete a database user."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    user = PostgresDatabaseUser.query.filter_by(
        id=user_id, 
        database_id=database_id
    ).first_or_404()
    
    if user.is_primary:
        flash('Cannot delete the primary user', 'error')
        return redirect(url_for('databases.database_users', database_id=database_id))
    
    # Delete user from server
    success, message = DatabaseService.execute_with_postgres(
        database.vps_server,
        'User deletion',
        DatabaseService.delete_user_operation,
        user.username
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.database_users', database_id=database_id))
    
    # Delete from database
    try:
        db.session.delete(user)
        db.session.commit()
        flash('User deleted successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to delete user from database: {str(e)}', 'error')
    
    return redirect(url_for('databases.database_users', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/delete', methods=['POST'])
@login_required
def delete_database(database_id):
    """Delete a database."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    try:
        # Delete all users first
        PostgresDatabaseUser.query.filter_by(database_id=database_id).delete()
        
        # Delete the database
        db.session.delete(database)
        db.session.commit()
        
        flash('Database deleted successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to delete database: {str(e)}', 'error')
    
    return redirect(url_for('databases.databases'))


@databases_bp.route('/check_postgres/<int:server_id>')
@login_required
def check_postgres(server_id):
    """Check PostgreSQL installation and version on server."""
    server = VpsServer.query.filter_by(
        id=server_id, 
        # Removed user_id for single-user mode
    ).first_or_404()
    
    result = DatabaseService.check_postgres_status(server)
    return jsonify(result)


@databases_bp.route('/databases/<int:database_id>/import', methods=['GET', 'POST'])
@login_required
def import_database(database_id):
    """Import database from external source."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    if request.method == 'GET':
        return render_template('import_database.html', database=database)
    
    # POST request - process import
    data = request.form.to_dict()
    
    # Determine connection method
    if data.get('connection_method') == 'url':
        connection_string = data.get('connection_url', '').strip()
        
        # Validate connection string
        url_valid, url_error = ValidationService.validate_connection_string(connection_string)
        if not url_valid:
            flash(url_error, 'error')
            return redirect(url_for('databases.import_database', database_id=database_id))
    else:
        # Build connection string from individual fields
        required_fields = ['source_host', 'source_port', 'source_username', 
                          'source_password', 'source_database']
        field_errors = ValidationService.validate_required_fields(data, required_fields)
        
        if field_errors:
            for error in field_errors:
                flash(error, 'error')
            return redirect(url_for('databases.import_database', database_id=database_id))
        
        connection_string = (
            f"postgresql://{data['source_username']}:{data['source_password']}"
            f"@{data['source_host']}:{data['source_port']}/{data['source_database']}"
        )
    
    # Create restore log
    try:
        restore_log = RestoreLog(
            database_id=database_id,
            status='in_progress',
            log_output='Import process started',
            created_at=datetime.utcnow()
        )
        db.session.add(restore_log)
        db.session.commit()
        
        flash('Database import started. You can check the progress below.', 'info')
        return redirect(url_for('databases.import_progress', 
                              database_id=database_id, 
                              restore_log_id=restore_log.id))
        
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to start import process: {str(e)}', 'error')
        return redirect(url_for('databases.import_database', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/import/<int:restore_log_id>/progress')
@login_required
def import_progress(database_id, restore_log_id):
    """Display import progress."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    restore_log = RestoreLog.query.filter_by(
        id=restore_log_id, 
        database_id=database_id
    ).first_or_404()
    
    return render_template('import_progress.html', 
                         database=database, 
                         restore_log=restore_log)


@databases_bp.route('/databases/<int:database_id>/import/<int:restore_log_id>/execute', methods=['POST'])
@login_required
def execute_import(database_id, restore_log_id):
    """Execute the database import process."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id,
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    restore_log = RestoreLog.query.filter_by(
        id=restore_log_id, 
        database_id=database_id
    ).first_or_404()
    
    if restore_log.status != 'in_progress':
        return jsonify({'success': False, 'message': 'Import is not in progress'})
    
    # Get connection string from request
    connection_string = request.json.get('connection_string')
    if not connection_string:
        return jsonify({'success': False, 'message': 'Connection string is required'})
    
    # Execute import
    success, message = DatabaseService.execute_with_postgres(
        database.vps_server,
        'Database import',
        DatabaseImportService.perform_database_import,
        connection_string, database.name, restore_log
    )
    
    # Update restore log status
    try:
        restore_log.status = 'completed' if success else 'failed'
        if not success:
            DatabaseImportService.update_log(restore_log, f'Import failed: {message}')
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Failed to update log: {str(e)}'})
    
    return jsonify({'success': success, 'message': message})


@databases_bp.route('/databases/<int:database_id>/import/<int:restore_log_id>/status')
@login_required
def import_status(database_id, restore_log_id):
    """Get import status."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    restore_log = RestoreLog.query.filter_by(
        id=restore_log_id, 
        database_id=database_id
    ).first_or_404()
    
    return jsonify({
        'status': restore_log.status,
        'log_output': restore_log.log_output,
        'created_at': restore_log.created_at.isoformat() if restore_log.created_at else None
    })