from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from app.models.database import VpsServer, PostgresDatabase, PostgresDatabaseUser, RestoreLog, db
from app.utils.database_service import DatabaseService, DatabaseImportService
from app.utils.unified_validation_service import UnifiedValidationService
from app.utils.permission_manager import PermissionManager
from datetime import datetime

databases_bp = Blueprint('databases', __name__)


@databases_bp.route('/databases')
@login_required
def databases():
    """Display all databases for the current user."""
    user_databases = PostgresDatabase.query.join(VpsServer).all()
    return render_template('databases/index.html', databases=user_databases)


@databases_bp.route('/databases/add', methods=['GET', 'POST'])
@login_required
def add_database():
    """Add a new database."""
    if request.method == 'GET':
        servers = VpsServer.query.all()
        return render_template('databases/add.html', servers=servers)
    
    # POST request - process form
    data = request.form.to_dict()
    
    # Validate required fields
    required_fields = ['vps_server_id', 'name', 'password']
    fields_valid, field_errors = UnifiedValidationService.validate_required_fields(data, required_fields)
    if not fields_valid:
        for error in field_errors:
            flash(error, 'error')
        return redirect(url_for('databases.add_database'))
    
    # Validate database name
    name_valid, name_error = UnifiedValidationService.validate_database_name(data['name'])
    if not name_valid:
        flash(name_error, 'error')
        return redirect(url_for('databases.add_database'))
    
    # Check if database already exists
    existing_db = PostgresDatabase.query.filter_by(
        name=data['name'],
        vps_server_id=int(data['vps_server_id'])
    ).first()
    if existing_db:
        flash('A database with this name already exists on the selected server', 'error')
        return redirect(url_for('databases.add_database'))
    
    # Get server and validate ownership
    server = VpsServer.query.filter_by(
        id=data['vps_server_id']
    ).first()
    
    if not server:
        flash('Invalid server selected', 'error')
        return redirect(url_for('databases.add_database'))
    
    # Generate username and use password from form
    existing_users = [user.username for user in PostgresDatabaseUser.query.all()]
    username = UnifiedValidationService.generate_username(data['name'], existing_users)
    password = data.get('password', '').strip()
    
    # Validate password
    password_valid, password_error = UnifiedValidationService.validate_password(password)
    if not password_valid:
        flash(password_error, 'error')
        return redirect(url_for('databases.add_database'))
    
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
    
    # Create primary user on server
    success, message = DatabaseService.execute_with_postgres(
        server,
        'Primary user creation',
        DatabaseService.create_user_operation,
        username, password, data['name'], 'read_write'
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
    """Edit primary database user (simplified to match secondary user editing)."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
    ).first_or_404()
    
    primary_user = PostgresDatabaseUser.query.filter_by(
        database_id=database.id, 
        is_primary=True
    ).first()
    
    if not primary_user:
        flash('Primary user not found', 'error')
        return redirect(url_for('databases.database_users', database_id=database_id))
    
    if request.method == 'GET':
        current_permission = DatabaseService.get_current_user_permission(
            database.server, database.name, primary_user.username
        )
        individual_permissions = DatabaseService.get_user_individual_permissions(
            database.server, database.name
        )
        user_individual_perms = individual_permissions.get(primary_user.username, {})
        
        # Get permission combinations for the UI
        permission_combinations = PermissionManager.get_permission_combinations()
        
        # Debug: Print actual permission values
        print(f"DEBUG: user_individual_perms for {primary_user.username}: {user_individual_perms}")
        
        # Detect current combination
        current_combination, is_exact_match, _ = PermissionManager.detect_combination_from_permissions_enhanced(user_individual_perms)
        
        print(f"DEBUG: detected combination: {current_combination}, exact_match: {is_exact_match}")
        
        return render_template('databases/edit_user.html', 
                             database=database, 
                             user=primary_user, 
                             current_permission=current_permission,
                             user_individual_permissions=user_individual_perms,
                             permission_combinations=permission_combinations,
                             individual_permissions=PermissionManager.get_individual_permissions(),
                             current_combination=current_combination,
                             is_exact_match=is_exact_match)
    
    # POST request - update permissions and/or regenerate password
    permission_combination = request.form.get('permission_combination')
    regenerate_password = request.form.get('regenerate_password') == 'on'
    
    # Validate permission combination if provided
    if permission_combination:
        valid_combinations = [combo['value'] for combo in PermissionManager.get_permission_combinations()]
        if permission_combination not in valid_combinations:
            flash('Invalid permission combination selected', 'error')
            return redirect(url_for('databases.edit_database', database_id=database_id))
    
    # Generate new password if requested
    new_password = primary_user.password
    if regenerate_password:
        new_password = UnifiedValidationService.generate_password()
    
    # Apply permission combination if provided
    if permission_combination:
        # Get permissions for the combination
        permissions = PermissionManager.get_permissions_for_combination(permission_combination)
        
        # Update permissions and password on server
        success, message = DatabaseService.execute_with_postgres(
            database.server,
            'Primary user update',
            DatabaseService.grant_individual_permissions_operation,
            primary_user.username, new_password, database.name, permissions
        )
    else:
        # If no permission combination provided, just update password
        success = True
        message = "Password updated successfully"
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.edit_database', database_id=database_id))
    
    # Update in database
    try:
        if regenerate_password:
            primary_user.password = new_password
        db.session.commit()
        
        if regenerate_password:
            flash(f'Primary user updated successfully. New password: {new_password}', 'success')
        else:
            flash('Primary user permissions updated successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to update primary user in database: {str(e)}', 'error')
    
    return redirect(url_for('databases.database_users', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/users/add', methods=['GET', 'POST'])
@login_required
def add_database_user(database_id):
    """Add a new user to a database."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
    ).first_or_404()
    
    if request.method == 'GET':
        # Get permission combinations and individual permissions for the UI
        permission_combinations = PermissionManager.get_permission_combinations(include_custom=False)
        individual_permissions = PermissionManager.get_individual_permissions()
        return render_template('databases/add_user.html', 
                             database=database,
                             permission_combinations=permission_combinations,
                             individual_permissions=individual_permissions)
    
    # POST request - process form
    data = request.form.to_dict()
    
    # Determine permission mode
    permission_mode = data.get('permission_mode', 'preset')
    
    # Validate required fields based on permission mode
    if permission_mode == 'preset':
        required_fields = ['username', 'permission_combination', 'password']
    else:  # individual mode
        required_fields = ['username', 'password']
    
    fields_valid, field_errors = UnifiedValidationService.validate_required_fields(data, required_fields)
    
    # Validate individual fields
    validations = [
        UnifiedValidationService.validate_username(data.get('username', ''))
    ]
    
    if not fields_valid:
        for error in field_errors:
            flash(error, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    if not UnifiedValidationService.validate_and_flash_errors(validations):
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Check if user already exists
    if UnifiedValidationService.validate_user_exists(data['username'], database_id):
        flash('A user with this username already exists for this database', 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Get password from form and validate it
    password = data.get('password', '').strip()
    password_valid, password_error = UnifiedValidationService.validate_password(password)
    if not password_valid:
        flash(password_error, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # First create the user
    success, message = DatabaseService.execute_with_postgres(
        database.server,
        'User creation',
        DatabaseService.create_user_operation,
        data['username'], password, database.name, 'no_access'  # Create with no permissions initially
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Handle permissions based on mode
    if permission_mode == 'preset':
        # Use preset permission combination
        permissions = PermissionManager.get_permissions_for_combination(data['permission_combination'])
    else:
        # Use individual permissions
        individual_permissions = {}
        for key in data:
            if key.startswith('individual_'):
                perm_name = key.replace('individual_', '')
                individual_permissions[perm_name] = data[key] == 'true'
        
        # Validate individual permissions
        validation_result = PermissionManager.validate_individual_permissions(individual_permissions)
        if not validation_result['valid']:
            flash(validation_result['message'], 'error')
            return redirect(url_for('databases.add_database_user', database_id=database_id))
        
        # Apply individual permissions
        permissions = PermissionManager.apply_individual_permissions(individual_permissions)
    
    # Grant the permissions
    success, message = DatabaseService.execute_with_postgres(
        database.server,
        'Permission assignment',
        DatabaseService.grant_individual_permissions_operation,
        data['username'], password, database.name, permissions
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.add_database_user', database_id=database_id))
    
    # Save to database
    try:
        new_user = PostgresDatabaseUser(
            username=data['username'],
            password=password,
            database_id=database_id,
            is_primary=False
        )
        db.session.add(new_user)
        db.session.commit()
        
        flash(f'User added successfully with password: {password}', 'success')
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
        PostgresDatabase.id == database_id
    ).first_or_404()
    
    users = PostgresDatabaseUser.query.filter_by(database_id=database_id).all()
    user_permissions = DatabaseService.get_user_permissions(database.server, database.name)
    
    # Get individual permissions for each user
    all_individual_permissions = DatabaseService.get_user_individual_permissions(database.server, database.name)
    
    # Detect permission combinations for each user
    user_permission_groups = {}
    for user in users:
        individual_perms = all_individual_permissions.get(user.username, {})
        combination, is_exact_match, _ = PermissionManager.detect_combination_from_permissions_enhanced(individual_perms)
        user_permission_groups[user.username] = {
            'combination': combination,
            'label': PermissionManager.get_combination_label(combination) if is_exact_match else 'Custom Permissions',
            'is_exact_match': is_exact_match
        }
    
    # Get primary user
    primary_user = PostgresDatabaseUser.query.filter_by(
        database_id=database_id, 
        is_primary=True
    ).first()
    
    # Generate connection strings if primary user exists
    connection_url = ''
    jdbc_url = ''
    ssl_enabled = False
    
    if primary_user:
        # Check if SSL is enabled on the server
        try:
            from app.utils.ssh_manager import SSHManager
            from app.utils.postgres_manager.core import PostgresManager
            
            ssh = SSHManager(
                host=database.server.host,
                port=database.server.port,
                username=database.server.username,
                ssh_key_content=database.server.ssh_key_content
            )
            
            if ssh.connect():
                pg_manager = PostgresManager(ssh)
                ssl_status = pg_manager.get_ssl_status()
                ssl_enabled = ssl_status.get('ssl_enabled', False)
                ssh.disconnect()
        except Exception:
            # If we can't check SSL status, default to non-SSL
            ssl_enabled = False
        
        # Generate connection strings with SSL support
        from app.utils.database_service import DatabaseImportService
        connection_url = DatabaseImportService.generate_connection_string(
            host=database.server.host,
            port=database.server.postgres_port,
            username=primary_user.username,
            password=primary_user.password,
            database=database.name,
            ssl_enabled=ssl_enabled
        )
        
        # JDBC URL with SSL support
        jdbc_url = f"jdbc:postgresql://{database.server.host}:{database.server.postgres_port}/{database.name}"
        if ssl_enabled:
            jdbc_url += "?ssl=true&sslmode=require"
    
    # Get list of available individual permissions
    individual_permissions = PermissionManager.get_individual_permissions()
    
    return render_template('databases/credentials.html', 
                         database=database, 
                         users=users, 
                         user_permissions=user_permissions,
                         user_permission_groups=user_permission_groups,
                         all_individual_permissions=all_individual_permissions,
                         individual_permissions=individual_permissions,
                         server=database.server,
                         primary_user=primary_user,
                         connection_url=connection_url,
                         jdbc_url=jdbc_url,
                         ssl_enabled=ssl_enabled)


@databases_bp.route('/databases/<int:database_id>/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_database_user(database_id, user_id):
    """Edit database user permissions."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
    ).first_or_404()
    
    user = PostgresDatabaseUser.query.filter_by(
        id=user_id, 
        database_id=database_id
    ).first_or_404()
    
    if request.method == 'GET':
        current_permission = DatabaseService.get_current_user_permission(
            database.server, database.name, user.username
        )
        individual_permissions = DatabaseService.get_user_individual_permissions(
            database.server, database.name
        )
        user_individual_perms = individual_permissions.get(user.username, {})
        
        # Debug: Print actual permission values
        print(f"DEBUG: user_individual_perms for {user.username}: {user_individual_perms}")
        
        # Get permission combinations and current combination
        permission_combinations = PermissionManager.get_permission_combinations(include_custom=False)
        individual_permissions = PermissionManager.get_individual_permissions()
        current_combination, is_exact_match, _ = PermissionManager.detect_combination_from_permissions_enhanced(user_individual_perms)
        
        print(f"DEBUG: detected combination: {current_combination}, exact_match: {is_exact_match}")
        
        return render_template('databases/edit_user.html', 
                             database=database, 
                             user=user, 
                             current_permission=current_permission,
                             user_individual_permissions=user_individual_perms,
                             permission_combinations=permission_combinations,
                             individual_permissions=individual_permissions,
                             current_combination=current_combination,
                             is_exact_match=is_exact_match)
    
    # POST request - update permissions and/or regenerate password
    data = request.form.to_dict()
    permission_mode = data.get('permission_mode', 'preset')
    permission_combination = data.get('permission_combination')
    regenerate_password = data.get('regenerate_password') == 'on'
    
    # Validate permission combination if provided
    if permission_mode == 'preset' and permission_combination:
        valid_combinations = [combo['value'] for combo in PermissionManager.get_permission_combinations(include_custom=False)]
        if permission_combination not in valid_combinations:
            flash('Invalid permission combination selected', 'error')
            return redirect(url_for('databases.edit_database_user', 
                                  database_id=database_id, user_id=user_id))
    
    # Generate new password if requested
    new_password = user.password
    if regenerate_password:
        new_password = UnifiedValidationService.generate_password()
    
    # Apply permissions based on mode
    permissions_updated = False
    if permission_mode == 'preset' and permission_combination:
        result = DatabaseService.apply_permission_combination(
            database.server, database.name, user.username, permission_combination
        )
        
        if not result['success']:
            flash(result['message'], 'error')
            return redirect(url_for('databases.edit_database_user', 
                                  database_id=database_id, user_id=user_id))
        permissions_updated = True
    elif permission_mode == 'individual':
        # Handle individual permissions
        individual_permissions = {}
        for key in data:
            if key.startswith('individual_'):
                perm_name = key.replace('individual_', '')
                individual_permissions[perm_name] = data[key] == 'true'
        
        # Validate individual permissions
        validation_result = PermissionManager.validate_individual_permissions(individual_permissions)
        if not validation_result['valid']:
            flash(validation_result['message'], 'error')
            return redirect(url_for('databases.edit_database_user', 
                                  database_id=database_id, user_id=user_id))
        
        # Apply individual permissions
        permissions = PermissionManager.apply_individual_permissions(individual_permissions)
        
        # Grant the permissions
        success, message = DatabaseService.execute_with_postgres(
            database.server,
            'Permission update',
            DatabaseService.grant_individual_permissions_operation,
            user.username, user.password, database.name, permissions
        )
        
        if not success:
            flash(f'Failed to update permissions: {message}', 'error')
            return redirect(url_for('databases.edit_database_user', 
                                  database_id=database_id, user_id=user_id))
        permissions_updated = True
    
    # Update password if regenerated
    if regenerate_password:
        success, message = DatabaseService.execute_with_postgres(
            database.server,
            'Password update',
            DatabaseService.update_user_password_operation,
            user.username, new_password
        )
        
        if not success:
            flash(f'Failed to update password: {message}', 'error')
            return redirect(url_for('databases.edit_database_user', 
                                  database_id=database_id, user_id=user_id))
    
    # Update in database
    try:
        if regenerate_password:
            user.password = new_password
        db.session.commit()
        
        if permissions_updated and regenerate_password:
            if permission_mode == 'preset' and permission_combination:
                combination_label = PermissionManager.get_combination_label(permission_combination)
                flash(f'User updated successfully. Applied "{combination_label}" permissions. New password: {new_password}', 'success')
            else:
                flash(f'User updated successfully. Applied individual permissions. New password: {new_password}', 'success')
        elif permissions_updated:
            if permission_mode == 'preset' and permission_combination:
                combination_label = PermissionManager.get_combination_label(permission_combination)
                flash(f'User permissions updated successfully. Applied "{combination_label}" permissions.', 'success')
            else:
                flash('User permissions updated successfully. Applied individual permissions.', 'success')
        elif regenerate_password:
            flash(f'User password updated successfully. New password: {new_password}', 'success')
        else:
            flash('No changes were made', 'info')
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to update user in database: {str(e)}', 'error')
    
    return redirect(url_for('databases.database_users', database_id=database_id))


@databases_bp.route('/databases/<int:database_id>/users/<int:user_id>/apply-combination', methods=['POST'])
@login_required
def apply_permission_combination(database_id, user_id):
    """Apply a predefined permission combination to a database user."""
    try:
        database = PostgresDatabase.query.get_or_404(database_id)
        user = PostgresDatabaseUser.query.get_or_404(user_id)
        
        # Validate that the user belongs to this database
        if user.database_id != database.id:
            return jsonify({
                'success': False,
                'message': 'User does not belong to this database'
            }), 400
        
        # Get the combination from request
        data = request.get_json()
        if not data or 'combination' not in data:
            return jsonify({
                'success': False,
                'message': 'Permission combination is required'
            }), 400
        
        combination = data['combination']
        
        # Validate combination
        valid_combinations = [combo['value'] for combo in PermissionManager.get_permission_combinations()]
        if combination not in valid_combinations:
            return jsonify({
                'success': False,
                'message': 'Invalid permission combination'
            }), 400
        
        # Apply the permission combination
        result = DatabaseService.apply_permission_combination(
            database.server, 
            database.name, 
            user.username, 
            combination
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': result['message'],
                'combination': result['combination'],
                'permissions': result['permissions']
            })
        else:
            return jsonify({
                'success': False,
                'message': result['message']
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error applying permission combination: {str(e)}'
        }), 500

@databases_bp.route('/databases/<int:database_id>/users/<int:user_id>/delete', methods=['POST'])
@login_required
def delete_database_user(database_id, user_id):
    """Delete a database user."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
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
        database.server,
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
        PostgresDatabase.id == database_id
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
        id=server_id
    ).first_or_404()
    
    result = DatabaseService.check_postgres_status(server)
    return jsonify(result)


@databases_bp.route('/databases/import', methods=['GET', 'POST'])
@login_required
def import_database():
    """Import database from external source and create new database."""
    if request.method == 'GET':
        servers = VpsServer.query.all()
        return render_template('databases/import.html', servers=servers)
    
    # POST request - process import
    data = request.form.to_dict()
    
    # Validate required fields for new database creation
    required_fields = ['vps_server_id', 'name', 'password']
    fields_valid, field_errors = UnifiedValidationService.validate_required_fields(data, required_fields)
    if not fields_valid:
        for error in field_errors:
            flash(error, 'error')
        return redirect(url_for('databases.import_database'))
    
    # Validate database name
    name_valid, name_error = UnifiedValidationService.validate_database_name(data['name'])
    if not name_valid:
        flash(name_error, 'error')
        return redirect(url_for('databases.import_database'))
    
    # Check if database already exists
    existing_db = PostgresDatabase.query.filter_by(
        name=data['name'],
        vps_server_id=int(data['vps_server_id'])
    ).first()
    if existing_db:
        flash('A database with this name already exists on the selected server', 'error')
        return redirect(url_for('databases.import_database'))
    
    # Get server and validate
    server = VpsServer.query.filter_by(id=data['vps_server_id']).first()
    if not server:
        flash('Invalid server selected', 'error')
        return redirect(url_for('databases.import_database'))
    
    # Generate username and validate password
    existing_users = [user.username for user in PostgresDatabaseUser.query.all()]
    username = UnifiedValidationService.generate_username(data['name'], existing_users)
    password = data.get('password', '').strip()
    
    password_valid, password_error = UnifiedValidationService.validate_password(password)
    if not password_valid:
        flash(password_error, 'error')
        return redirect(url_for('databases.import_database'))
    
    # Determine source connection method
    if data.get('connection_type') == 'url':
        connection_string = data.get('connection_url', '').strip()
        
        # Validate connection string
        url_valid, url_error = UnifiedValidationService.validate_connection_string(connection_string)
        if not url_valid:
            flash(url_error, 'error')
            return redirect(url_for('databases.import_database'))
    else:
        # Build connection string from individual fields
        source_fields = ['host', 'port', 'username', 'password', 'database_name']
        source_data = {f'source_{field}': data.get(field, '') for field in source_fields}
        
        source_field_errors = UnifiedValidationService.validate_required_fields(source_data, [f'source_{field}' for field in source_fields])
        
        if source_field_errors[1]:  # If there are errors
            for error in source_field_errors[1]:
                flash(error.replace('source_', ''), 'error')
            return redirect(url_for('databases.import_database'))
        
        connection_string = (
            f"postgresql://{data['username']}:{data['password']}"
            f"@{data['host']}:{data['port']}/{data['database_name']}"
        )
    
    # Create database on server
    success, message = DatabaseService.execute_with_postgres(
        server, 
        'Database creation',
        DatabaseService.create_database_operation,
        data['name'], username, password
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.import_database'))
    
    # Create primary user on server
    success, message = DatabaseService.execute_with_postgres(
        server,
        'Primary user creation',
        DatabaseService.create_user_operation,
        username, password, data['name'], 'read_write'
    )
    
    if not success:
        flash(message, 'error')
        return redirect(url_for('databases.import_database'))
    
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
            is_primary=True
        )
        db.session.add(new_user)
        db.session.commit()
        
        # Create restore log for import process
        restore_log = RestoreLog(
            database_id=new_database.id,
            status='in_progress',
            log_output='Database created successfully. Starting import process...',
            created_at=datetime.utcnow()
        )
        db.session.add(restore_log)
        db.session.commit()
        
        flash('Database created successfully. Import process started.', 'success')
        # Store connection string in session for the progress page
        from flask import session
        session['import_connection_string'] = connection_string
        session['import_connection_type'] = data.get('connection_type', 'standard')
        if data.get('connection_type') != 'url':
            session['import_connection_data'] = {
                'host': data.get('host'),
                'port': data.get('port'),
                'username': data.get('username'),
                'password': data.get('password'),
                'database_name': data.get('database_name')
            }
        
        return redirect(url_for('databases.import_progress', 
                              database_id=new_database.id, 
                              restore_log_id=restore_log.id))
        
    except Exception as e:
        db.session.rollback()
        flash(f'Failed to create database: {str(e)}', 'error')
        return redirect(url_for('databases.import_database'))


@databases_bp.route('/databases/<int:database_id>/import/<int:restore_log_id>/progress')
@login_required
def import_progress(database_id, restore_log_id):
    """Display import progress."""
    from flask import session
    
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
    ).first_or_404()
    
    restore_log = RestoreLog.query.filter_by(
        id=restore_log_id, 
        database_id=database_id
    ).first_or_404()
    
    # Get connection data from session
    connection_string = session.get('import_connection_string', '')
    connection_type = session.get('import_connection_type', 'standard')
    connection_data = session.get('import_connection_data', {})
    
    template_data = {
        'database': database,
        'restore_log': restore_log,
        'connection_string': connection_string,
        'connection_type': connection_type
    }
    
    # Add connection data if standard connection
    if connection_type == 'standard':
        template_data.update({
            'host': connection_data.get('host', ''),
            'port': connection_data.get('port', 5432),
            'username': connection_data.get('username', ''),
            'password': connection_data.get('password', ''),
            'database_name': connection_data.get('database_name', '')
        })
    else:
        template_data['connection_url'] = connection_string
    
    return render_template('databases/import_progress.html', **template_data)


@databases_bp.route('/databases/<int:database_id>/import/<int:restore_log_id>/execute', methods=['POST'])
@login_required
def execute_import(database_id, restore_log_id):
    """Execute the database import process."""
    database = PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
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
        database.server,
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
    # Validate database exists
    PostgresDatabase.query.join(VpsServer).filter(
        PostgresDatabase.id == database_id
        # Removed user_id filtering for single-user mode
    ).first_or_404()
    
    restore_log = RestoreLog.query.filter_by(
        id=restore_log_id, 
        database_id=database_id
    ).first_or_404()
    
    return jsonify({
        'success': True,
        'status': restore_log.status,
        'log_output': restore_log.log_output or '',
        'is_complete': restore_log.status in ['completed', 'failed'],
        'created_at': restore_log.created_at.isoformat() if restore_log.created_at else None
    })