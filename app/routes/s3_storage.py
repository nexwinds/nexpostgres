from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from app.models.database import S3Storage, db
from app.routes.auth import first_login_required
import boto3
from botocore.exceptions import ClientError

s3_storage_bp = Blueprint('s3_storage', __name__, url_prefix='/s3-storage')

@s3_storage_bp.route('/')
@login_required
@first_login_required
def index():
    storages = S3Storage.query.all()
    return render_template('s3_storage/index.html', storages=storages)

@s3_storage_bp.route('/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    if request.method == 'POST':
        name = request.form.get('name')
        bucket = request.form.get('bucket')
        region = request.form.get('region')
        endpoint = request.form.get('endpoint')
        access_key = request.form.get('access_key')
        secret_key = request.form.get('secret_key')
        
        # Validate data
        if not name or not bucket or not region or not access_key or not secret_key:
            flash('All fields are required', 'danger')
            return render_template('s3_storage/add.html')
        
        # Check if name already exists for current user
        if S3Storage.query.filter_by(name=name).first():
            flash('A storage configuration with this name already exists', 'danger')
            return render_template('s3_storage/add.html')
        
        # Create S3 storage
        storage = S3Storage(
            name=name,
            bucket=bucket,
            region=region,
            endpoint=endpoint if endpoint else None,
            access_key=access_key,
            secret_key=secret_key,
            # Removed user_id for single-user mode
        )
        
        db.session.add(storage)
        db.session.commit()
        
        flash('S3 storage configuration added successfully', 'success')
        return redirect(url_for('s3_storage.index'))
    
    return render_template('s3_storage/add.html')

@s3_storage_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    storage = S3Storage.query.filter_by(id=id).first_or_404()
    
    if request.method == 'POST':
        name = request.form.get('name')
        bucket = request.form.get('bucket')
        region = request.form.get('region')
        endpoint = request.form.get('endpoint')
        access_key = request.form.get('access_key')
        secret_key = request.form.get('secret_key')
        
        # Validate data
        if not name or not bucket or not region or not access_key:
            flash('All fields are required', 'danger')
            return render_template('s3_storage/edit.html', storage=storage)
        
        # Check if name already exists for current user (excluding current)
        existing = S3Storage.query.filter_by(name=name).first()
        if existing and existing.id != storage.id:
            flash('A storage configuration with this name already exists', 'danger')
            return render_template('s3_storage/edit.html', storage=storage)
        
        # Update storage
        storage.name = name
        storage.bucket = bucket
        storage.region = region
        storage.endpoint = endpoint if endpoint else None
        storage.access_key = access_key
        
        # Only update secret key if provided
        if secret_key:
            storage.secret_key = secret_key
        
        db.session.commit()
        
        flash('S3 storage configuration updated successfully', 'success')
        return redirect(url_for('s3_storage.index'))
    
    return render_template('s3_storage/edit.html', storage=storage)

@s3_storage_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
@first_login_required
def delete(id):
    storage = S3Storage.query.filter_by(id=id).first_or_404()
    
    # Check if storage is being used by any backup job
    if storage.backup_jobs:
        flash('Cannot delete storage configuration that is being used by backup jobs', 'danger')
        return redirect(url_for('s3_storage.index'))
    
    db.session.delete(storage)
    db.session.commit()
    
    flash('S3 storage configuration deleted successfully', 'success')
    return redirect(url_for('s3_storage.index'))

@s3_storage_bp.route('/test-connection', methods=['POST'])
@login_required
@first_login_required
def test_connection():
    bucket = request.form.get('bucket')
    region = request.form.get('region')
    access_key = request.form.get('access_key')
    secret_key = request.form.get('secret_key')
    use_stored_key = request.form.get('use_stored_key')
    storage_id = request.form.get('storage_id', type=int)
    
    # If using stored key, get it from the database (ensure user owns it)
    if use_stored_key and storage_id:
        storage = S3Storage.query.filter_by(id=storage_id).first()
        if storage:
            secret_key = storage.secret_key
    
    if not bucket or not region or not access_key or not secret_key:
        return jsonify({'success': False, 'message': 'All fields are required'})
    
    try:
        # Create S3 client
        s3 = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        # Try to list objects in the bucket to test connection
        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        
        return jsonify({'success': True, 'message': 'Connection successful'})
    except ClientError as e:
        error_message = str(e)
        return jsonify({'success': False, 'message': f'Connection failed: {error_message}'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})