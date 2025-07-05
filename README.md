# NEXPOSTGRES - PostgreSQL Remote Backup Manager

NEXPOSTGRES is a centralized control panel for managing PostgreSQL databases running on remote Ubuntu VPS servers over SSH. It automates backup procedures using pgBackRest and allows you to schedule and monitor backups, as well as perform restores when needed.

## Features

- **Authentication**: Simple admin user login system with enforced password change on first login
- **VPS Server Management**: 
  - Add, edit, and delete remote Ubuntu VPS servers with SSH access
  - Automatic server initialization with PostgreSQL and pgBackRest installation
  - PostgreSQL port configuration at the server level for all databases
- **Database Management**: Track PostgreSQL databases across your VPS servers
- **Automated Backup Management**: 
  - Configure and schedule full and incremental backups
  - Setup Amazon S3 backup storage
  - Automatically install and configure pgBackRest on remote servers
- **Restore Capabilities**: Restore databases from backups, including point-in-time recovery
- **Monitoring Dashboard**: Track backup status, history, and logs

## Requirements

- Docker and Docker Compose
- Remote Ubuntu VPS servers with:
  - SSH access using key authentication
  - Sudo privileges
  - Compatible with PostgreSQL and pgBackRest
- Amazon S3 bucket for backup storage (with proper credentials)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/nexpostgres.git
   cd nexpostgres
   ```

2. Configure the application:
   - Edit `docker-compose.yml` to set a secure `SECRET_KEY`

3. Create necessary directories:
   ```
   mkdir -p data app/ssh_keys app/flask_session
   ```

4. Build and start the application:
   ```
   docker-compose up -d
   ```

5. Access the web interface at http://localhost:5000
   - Default login: `admin` / `admin`
   - You'll be prompted to change the password on first login

## Usage

### Adding a VPS Server

1. Go to "VPS Servers" and click "Add Server"
2. Enter server details including hostname, SSH port, username
3. Provide SSH key information (file path or key content)
4. Test the connection before saving
5. The server will automatically be initialized with PostgreSQL and pgBackRest upon creation
   - This process includes: updating system packages, installing PostgreSQL and pgBackRest, and restarting services
   - If automatic initialization fails, you can retry it from the server list page

### Adding a PostgreSQL Database

1. Go to "Databases" and click "Add Database"
2. Select the VPS server where the database is hosted
3. Enter database name, port, username and password

### Configuring Backups

1. Go to "Backups" → "Backup Jobs" and click "Add Backup Job"
2. Select the database to back up
3. Choose backup type (full/incremental) and schedule (using cron expression)
4. Provide S3 configuration details
5. Save the backup job and set up pgBackRest configuration on the server

### Restoring a Database

1. Go to "Backups" → "Restore Database" 
2. Select the database to restore
3. Choose a backup to restore from or specify a point in time
4. Confirm and execute the restore

## Architecture

- **Flask Web Application**: Provides the web interface and API endpoints
- **SQLite Database**: Stores configuration data, server info, and backup logs
- **SSH Connections**: Uses paramiko to connect and execute commands on remote servers
- **pgBackRest**: Handles the actual backup/restore operations on remote servers
- **APScheduler**: Manages scheduled backup jobs

## Security Notes

- Change the default admin password immediately after first login
- Use strong SSH keys for VPS server authentication
- Set a secure `SECRET_KEY` in the Docker environment variables
- Ensure S3 credentials have appropriate permissions
- For production use, consider implementing additional security measures like HTTPS

## License

[GPLv3 License](LICENSE) 