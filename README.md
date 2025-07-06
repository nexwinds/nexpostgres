# NEXPOSTGRES - PostgreSQL Remote Management Platform

NEXPOSTGRES is a centralized control panel for managing PostgreSQL databases running on remote Ubuntu VPS servers over SSH. It automates backup procedures using pgBackRest and allows you to schedule and monitor backups, as well as perform restores when needed.

![NEXPOSTGRES Dashboard](https://via.placeholder.com/800x400?text=NEXPOSTGRES+Dashboard)

## Features

- **Authentication System**
  - Secure admin user login with session management
  - Enforced password change on first login
  - Session timeout protection

- **VPS Server Management**
  - Add, edit, and delete remote Ubuntu VPS servers with SSH access
  - Automatic server initialization with PostgreSQL and pgBackRest installation
  - Server health monitoring and status checks
  - PostgreSQL port configuration at the server level

- **Database Management**
  - Track and manage PostgreSQL databases across multiple VPS servers
  - Credential management with encryption
  - Database connection testing

- **Automated Backup Management**
  - Configure and schedule full and incremental backups
  - Setup Amazon S3 backup storage with secure credential handling
  - Automatically install and configure pgBackRest on remote servers
  - Flexible scheduling using cron expressions

- **Restore Capabilities**
  - Restore databases from any available backup point
  - Point-in-time recovery (PITR) support
  - Non-destructive restore options (to alternate locations)

- **Monitoring Dashboard**
  - Real-time backup job status
  - Comprehensive backup history logs
  - Server resource utilization monitoring

## Requirements

- Docker and Docker Compose for hosting the application
- Remote Ubuntu VPS servers with:
  - SSH access using key authentication
  - Sudo privileges
  - Compatible with PostgreSQL (v17+) and pgBackRest
- Amazon S3 bucket for backup storage with:
  - Proper IAM credentials (access key and secret)
  - Appropriate bucket permissions configured

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/nexwinds/nexpostgres.git
   cd nexpostgres
   ```

2. Create necessary directories:
   ```bash
   mkdir -p data app/ssh_keys app/flask_session
   ```

3. Configure the application:
   ```bash
   # Edit docker-compose.yml to set environment variables
   nano docker-compose.yml
   
   # Important: Set a secure SECRET_KEY and other configuration options
   ```

4. Build and start the application:
   ```bash
   docker-compose up -d
   ```

5. Access the web interface at http://localhost:5000
   - Default login: `admin` / `admin`
   - You'll be prompted to change the password on first login

## Configuration

### Environment Variables

Key environment variables in `docker-compose.yml`:

```yaml
environment:
  - SECRET_KEY=your_secure_random_string
  - DEBUG=False
  - ALLOWED_HOSTS=localhost,127.0.0.1
  - DB_PATH=/app/data/nexpostgres.db
```

### Persistent Storage

The application uses volumes to persist data:

```yaml
volumes:
  - ./data:/app/data
  - ./app/ssh_keys:/app/ssh_keys
  - ./app/flask_session:/app/flask_session
```

## Usage

### Adding a VPS Server

1. Navigate to "VPS Servers" and click "Add Server"
2. Enter server details:
   - Hostname (IP or domain)
   - SSH port (default: 22)
   - Username with sudo privileges
   - PostgreSQL port to use
3. Provide SSH key authentication:
   - Paste key content
   - Note: Password-based authentication is not supported
4. Test the connection before saving
5. The server will be automatically initialized with:
   - PostgreSQL installation/configuration
   - pgBackRest setup
   - Required system dependencies

### Managing PostgreSQL Databases

1. Go to "Databases" and click "Add Database"
2. Select the VPS server where the database is hosted
3. Enter database details:
   - Database name
   - Username and password
4. Test the connection before saving
5. View all databases organized by server from the main Databases page

### Configuring Backup Jobs

1. Navigate to "Backups" → "Backup Jobs" and click "Add Backup Job"
2. Select the database to back up
3. Configure backup settings:
   - Backup type (full/incremental/differential)
   - Schedule using cron expression (e.g., `0 2 * * *` for daily at 2 AM)
   - Retention policy (how many backups to keep)
4. Configure S3 storage:
   - Bucket name
   - Access key and secret
   - Region
5. Save the backup job, which will automatically:
   - Create pgBackRest configuration on the server
   - Schedule the backup job in the system
   - Perform an initial test backup

### Restoring a Database

1. Go to "Backups" → "Restore Database"
2. Select the database to restore
3. Choose a restore method:
   - Latest backup
   - Specific backup point by date/time
   - Point-in-time recovery (specify exact timestamp)
4. Select restore options:
   - Restore to original database (overwrites existing data)
   - Restore to a new database (specify name)
5. Confirm and execute the restore operation
6. Monitor restore progress and view logs

## Monitoring

### Dashboard

The dashboard provides:
- Server status overview with health indicators
- Recent backup job status
- Failed job alerts
- Storage utilization metrics

### Logs

Access detailed logs for:
- Backup job execution
- Restore operations
- Server initialization processes
- System errors and warnings

## Troubleshooting

### Common Issues

1. **SSH Connection Failures**
   - Verify SSH key permissions (should be 600)
   - Check that the server's SSH service is running
   - Ensure the user has sudo privileges

2. **PostgreSQL Errors**
   - Verify PostgreSQL is properly installed and running
   - Check database user permissions
   - Ensure the specified port is open in the server firewall

3. **Backup Failures**
   - Check S3 credentials and permissions
   - Verify sufficient disk space on the server
   - Review pgBackRest configuration

### Getting Help

For additional assistance:
- Check the application logs in the Docker container
- Run diagnostic tests from the server management page
- Visit our [GitHub issues page](https://github.com/nexwinds/nexpostgres/issues)

## Architecture

- **Flask Web Application**: Provides the web interface and API endpoints
- **SQLite Database**: Stores configuration data, server info, and backup logs
- **Paramiko SSH Client**: Manages secure connections to remote servers
- **pgBackRest**: Handles the actual backup/restore operations on remote servers
- **APScheduler**: Manages scheduled backup jobs
- **Docker Container**: Provides isolated, reproducible environment

## Security Best Practices

- Change the default admin password immediately after first login
- Use strong SSH keys (ED25519 or RSA 4096+) for VPS server authentication
- Set a secure random `SECRET_KEY` in the Docker environment variables
- Ensure S3 credentials follow the principle of least privilege
- Consider placing the application behind a reverse proxy with HTTPS
- Regularly update the application and underlying Docker images
- Implement network-level security (firewall rules, VPN, etc.)

## Upgrading

To upgrade to a newer version:

1. Pull the latest code:
   ```bash
   git pull origin main
   ```

2. Rebuild and restart containers:
   ```bash
   docker-compose down
   docker-compose build
   docker-compose up -d
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the [GPLv3 License](LICENSE)

## Contact

Project Link: [https://github.com/nexwinds/nexpostgres](https://github.com/yourusername/nexpostgres) 