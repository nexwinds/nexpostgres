# NEXPOSTGRES - PostgreSQL Remote Management Platform

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/flask-2.3.3-green.svg)](https://flask.palletsprojects.com/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

NEXPOSTGRES is a comprehensive, web-based PostgreSQL database management platform for managing PostgreSQL databases on remote Ubuntu VPS servers over SSH. It provides enterprise-grade backup automation using pgBackRest with scheduling, monitoring, and point-in-time recovery.

## 🚀 Key Benefits

- **🐳 Easy Setup** - Docker-based deployment in minutes
- **📈 Scalable** - Manage multiple servers and databases from unified dashboard
- **💾 Intelligent Backups** - Automated full, incremental, and differential strategies
- **🔒 Enterprise Security** - AES-256-CBC encryption, SSH key authentication
- **⚡ Lightweight** - Minimal resource consumption on production servers
- **🔄 Point-in-Time Recovery** - Restore to any point in time with pgBackRest

## ✨ Features

### 🔐 Authentication & Security
- Secure admin authentication with configurable timeout
- SSH key management and encrypted credential storage
- Enforced security policies and mandatory password changes

### 🖥️ VPS Server Management
- Multi-server support for unlimited Ubuntu VPS servers
- Automated PostgreSQL and pgBackRest installation
- Support for PostgreSQL versions 15, 16, and 17
- Real-time health monitoring and SSH connection testing

### 🗄️ Database Management
- Multi-database support across servers
- Connection testing and user management
- Import/export functionality with progress tracking
- Encrypted storage of database credentials

### 💾 Advanced Backup Management
- Full, incremental, and differential backup strategies
- Cron-based scheduling with visual expression builder
- Amazon S3 integration with encryption at rest
- Configurable retention policies with automatic cleanup
- LZ4 compression and AES-256-CBC encryption

### 🔄 Restore Capabilities
- Point-in-Time Recovery (PITR) to any timestamp
- Flexible restore options (original location or new database)
- Non-destructive restores with comprehensive logging
- Real-time progress monitoring

### 📊 Monitoring & Logging
- Unified dashboard with real-time status
- Comprehensive logging for all operations
- Health checks and alert system
- Historical data with searchable logs

## 📋 Requirements

### Host System
- **OS**: Linux, macOS, or Windows with Docker support
- **Docker**: Version 20.10+ with Docker Compose v2.0+
- **Memory**: 512MB RAM minimum (1GB+ recommended)
- **Storage**: 1GB+ free space
- **Network**: Internet access for Docker and S3 connectivity

### Remote VPS Servers
- **OS**: Ubuntu 18.04+ (20.04 LTS or 22.04 LTS recommended)
- **Architecture**: x86_64 (AMD64)
- **Memory**: 1GB RAM minimum (2GB+ recommended)
- **Network**: SSH access and internet connectivity
- **User**: SSH key-based authentication with sudo privileges

### Amazon S3 Storage
- Dedicated S3 bucket for backup storage
- IAM credentials with required permissions:
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
  - `s3:ListBucket`, `s3:GetBucketLocation`
- Versioning enabled (recommended)

### PostgreSQL Compatibility
- **Versions**: PostgreSQL 15.x, 16.x, 17.x
- **pgBackRest**: Automatically installed (version 2.40+)
- **Extensions**: None required (base installation sufficient)

## 🚀 Installation

### Quick Start with Docker (Recommended)

1. **Clone and setup:**
   ```bash
   git clone https://github.com/nexwinds/nexpostgres.git
   cd nexpostgres
   mkdir -p data app/ssh_keys app/flask_session
   chmod 700 app/ssh_keys
   ```

2. **Configure environment (edit docker-compose.yml):**
   ```yaml
   environment:
     - SECRET_KEY=your-secure-random-256-bit-key-here  # CHANGE THIS!
     - LOG_LEVEL=INFO
     - SESSION_LIFETIME_HOURS=24
     - DEFAULT_POSTGRES_PORT=5432
     - SCHEDULER_TIMEZONE=UTC
   ```

3. **Start application:**
   ```bash
   docker-compose up -d
   ```

4. **Access interface:**
   - URL: http://localhost:5000
   - Default credentials: `admin` / `admin`
   - **Change password on first login**

### Local Development Setup

```bash
# Prerequisites: Python 3.9+
git clone https://github.com/nexwinds/nexpostgres.git
cd nexpostgres

# Setup virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Create directories
mkdir -p app/ssh_keys app/flask_session data
chmod 700 app/ssh_keys

# Set environment variables
export SECRET_KEY="your-development-secret-key"
export FLASK_DEBUG=1

# Run application
python3 run.py
```

### Production Deployment

1. **Security hardening:**
   ```bash
   # Generate secure secret key
   python3 -c "import secrets; print(secrets.token_hex(32))"
   # Update docker-compose.yml with generated key
   ```

2. **Reverse proxy (Nginx example):**
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;
       location / {
           proxy_pass http://localhost:5000;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
       }
   }
   ```

3. **SSL/TLS:**
   ```bash
   sudo apt install certbot python3-certbot-nginx
   sudo certbot --nginx -d your-domain.com
   ```

## ⚙️ Configuration

### Environment Variables

```yaml
environment:
  # Security Settings
  - SECRET_KEY=your_secure_random_256_bit_key          # REQUIRED
  - SESSION_COOKIE_SECURE=0                           # Set to 1 for HTTPS
  - SESSION_LIFETIME_HOURS=24
  
  # Application Settings
  - FLASK_DEBUG=0                                     # Set to 1 for development
  - LOG_LEVEL=INFO                                    # DEBUG, INFO, WARNING, ERROR
  - DATABASE_URI=sqlite:///nexpostgres.sqlite
  
  # Default Values
  - DEFAULT_SSH_PORT=22
  - DEFAULT_POSTGRES_PORT=5432
  - DEFAULT_BACKUP_RETENTION_COUNT=7
  
  # Scheduler Settings
  - SCHEDULER_TIMEZONE=UTC
```

### Persistent Storage

```yaml
volumes:
  - ./data:/app/data                    # Application database and logs
  - ./app/ssh_keys:/app/app/ssh_keys    # SSH private keys (secure)
  - ./app/flask_session:/app/app/flask_session  # Session data
```

### Security Configuration

```bash
# SSH key security
chmod 700 app/ssh_keys
chmod 600 app/ssh_keys/*.pem

# Network security
# Configure SESSION_COOKIE_SECURE=1 for HTTPS
# Use strong SECRET_KEY (256-bit recommended)
```

## 📖 Usage Guide

### Adding a VPS Server

1. **Navigate to "VPS Servers" → "Add Server"**

2. **Enter server details:**
   ```
   Server Name: My Production Server
   Hostname/IP: 192.168.1.100
   SSH Port: 22
   Username: ubuntu
   PostgreSQL Port: 5432
   PostgreSQL Version: 17
   ```

3. **Configure SSH authentication:**
   ```bash
   # Generate SSH key pair
   ssh-keygen -t ed25519 -C "nexpostgres@yourhost"
   
   # Add public key to server
   ssh-copy-id -i ~/.ssh/id_ed25519.pub ubuntu@your-server-ip
   
   # Paste private key content in NEXPOSTGRES form
   ```

4. **Test connection and initialize**
   - Click "Test Connection" to verify SSH access
   - NEXPOSTGRES will automatically install PostgreSQL and pgBackRest

### Managing PostgreSQL Databases

1. **Add database:**
   - Navigate to "Databases" → "Add Database"
   - Select target VPS server
   - Enter database name, username, and password

2. **Test connection:**
   - Click "Test Connection" to verify credentials
   - Database passwords are encrypted before storage

### Configuring Backup Jobs

#### Step 1: Setup S3 Storage
1. Navigate to "S3 Storage" → "Add S3 Storage"
2. Configure S3 settings:
   ```
   Configuration Name: Production S3 Backup
   Bucket Name: my-postgres-backups
   Region: us-east-1
   Access Key ID: AKIA...
   Secret Access Key: your-secret-key
   ```

#### Step 2: Create Backup Job
1. Navigate to "Backups" → "Backup Jobs" → "Add Backup Job"
2. Select database and configure:
   ```
   Job Name: MyApp Production Daily Backup
   Backup Type: full
   S3 Storage: Production S3 Backup
   Retention Count: 7
   Schedule: 0 2 * * *  (Daily at 2:00 AM)
   ```

**Note:** Each database can have only ONE backup job (one-to-one relationship)

### Restoring a Database

1. **Navigate to "Backups" → "Restore Database"**

2. **Choose restore method:**
   - **Latest Backup**: Most recent successful backup
   - **Specific Backup**: Select from available timestamps
   - **Point-in-Time Recovery**: Specify exact timestamp

3. **Configure restore target:**
   - **Original Database**: ⚠️ Overwrites existing data
   - **New Database**: ✅ Safe option, creates new database

4. **Execute and verify:**
   - Review summary and start restore
   - Monitor real-time progress
   - Verify data integrity post-restore

## 📊 Monitoring

### Dashboard Overview
- 🟢 Server health indicators (online/offline status)
- 📊 PostgreSQL service status and disk utilization
- ✅ Recent backup job status and failures
- ⏱️ Currently running operations
- 🚨 Critical alerts and warnings

### Logging
- **Backup logs**: Detailed operation logs with compression ratios
- **Restore logs**: PITR and restore operation tracking
- **System logs**: SSH connections, configuration changes, security events
- **Log management**: Searchable interface with filtering and export

## 🔧 Troubleshooting

### SSH Connection Issues

**"SSH Connection Failed":**
```bash
# Check SSH key permissions
chmod 600 app/ssh_keys/*.pem

# Test SSH manually
ssh -i app/ssh_keys/server_key.pem ubuntu@your-server-ip
```

**"Permission Denied (publickey)":**
- Ensure public key is in `~/.ssh/authorized_keys` on remote server
- Verify username and SSH key authentication is enabled

### PostgreSQL Issues

**"Database Connection Failed":**
```sql
-- Check database and user permissions
\l
\du
GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;
```

**"PostgreSQL Service Not Running":**
```bash
sudo systemctl status postgresql
sudo systemctl start postgresql
```

### Backup Issues

**"Backup Job Failed":**
```bash
# Check pgBackRest configuration
sudo -u postgres pgbackrest --stanza=mydb check

# View logs
sudo tail -f /var/log/pgbackrest/pgbackrest.log
```

**"S3 Access Denied":**
- Verify S3 credentials and bucket permissions
- Check bucket region specification
- Test with AWS CLI: `aws s3 ls s3://your-bucket-name`

### Docker Issues

**"Container Won't Start":**
```bash
# Check logs and rebuild
docker-compose logs nexpostgres
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────┐
│              NEXPOSTGRES                │
│           (Docker Container)            │
├─────────────────────────────────────────┤
│ Web Interface │ API │ Authentication    │
│ SSH Manager   │ DB  │ Backup Service    │
│ Scheduler     │ Log │ File Manager      │
│ SQLite DB     │ Session Store           │
└─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────┐
│ Remote VPS │ Amazon S3 │ PostgreSQL     │
│ Servers    │ Storage   │ Databases      │
└─────────────────────────────────────────┘
```

### Core Components
- **Web Interface**: Flask application with responsive design
- **SSH Manager**: Secure connections using Paramiko with key-based auth
- **Database Manager**: PostgreSQL operations and connection management
- **Backup Service**: pgBackRest integration with S3 storage
- **Scheduler**: APScheduler for cron-like job scheduling
- **Security**: AES-256-CBC encryption and secure credential storage

### Data Flow
1. User Interaction → Web Interface → Authentication
2. Server Management → SSH Manager → Remote VPS
3. Database Operations → PostgreSQL Manager → Database
4. Backup Jobs → Scheduler → pgBackRest → S3 Storage
5. Monitoring → Log Manager → Dashboard Updates

## 🔒 Security Best Practices

### Authentication & Access
- Change default credentials immediately
- Use strong passwords (12+ characters)
- Configure appropriate session timeouts
- Generate strong SSH keys (4096-bit RSA or Ed25519)

### Network Security
```bash
# Firewall configuration
sudo ufw allow from YOUR_IP to any port 5432  # PostgreSQL
sudo ufw allow from TRUSTED_IPS to any port 5000  # NEXPOSTGRES
```

### Database Security
```sql
-- Create dedicated backup user
CREATE USER backup_user WITH PASSWORD 'strong_password';
GRANT CONNECT ON DATABASE mydb TO backup_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backup_user;
```

### S3 Security
- Use IAM user with minimal required permissions
- Enable server-side encryption
- Configure bucket policies for restricted access

### Container Security
```yaml
# docker-compose.yml security
security_opt:
  - no-new-privileges:true
user: "1000:1000"  # Non-root user
cap_drop:
  - ALL
```

## 🔄 Upgrading

### Pre-Upgrade Backup
```bash
# Stop application and backup
docker-compose down
BACKUP_DIR="nexpostgres_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -r data/ app/ssh_keys/ app/flask_session/ docker-compose.yml "$BACKUP_DIR/"
```

### Upgrade Methods

**Git-based (Recommended):**
```bash
git pull origin main
docker-compose build --no-cache
docker-compose up -d
```

**Docker Image:**
```bash
docker-compose pull
docker-compose up -d
```

### Post-Upgrade Verification
```bash
# Check container status
docker-compose ps

# Verify application health
curl -f http://localhost:5000/health

# Check logs for errors
docker-compose logs nexpostgres --since=5m | grep -i error
```

### Rollback Procedure
```bash
# If upgrade fails
docker-compose down
cp -r "$BACKUP_DIR"/* .
docker-compose up -d
```

## 🤝 Contributing

### Quick Start
1. Fork the repository
2. Clone your fork locally
3. Set up development environment
4. Make your changes
5. Submit a pull request

### Development Setup
```bash
git clone https://github.com/YOUR_USERNAME/nexpostgres.git
cd nexpostgres
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
pre-commit install
```

### Contribution Guidelines
- Follow existing code style
- Add tests for new functionality
- Update documentation as needed
- Use conventional commit messages:
  - `feat:` New feature
  - `fix:` Bug fix
  - `docs:` Documentation changes

### Testing
```bash
# Run tests
python -m pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run linting
flake8 app/
black app/
```

## 📄 License

NEXPOSTGRES is licensed under the **GPLv3 License**.

### License Summary
✅ Commercial use, modification, distribution, private use  
❌ No warranty or liability  
📋 **Requirements**: Include license, state changes, disclose source, same license for derivatives

### Third-Party Dependencies
- **Flask** (BSD-3-Clause) - Web framework
- **PostgreSQL** (PostgreSQL License) - Database system
- **pgBackRest** (MIT) - Backup solution
- **Docker** (Apache 2.0) - Containerization

---

## 🌟 NEXPOSTGRES

<div align="center">

**Professional PostgreSQL Backup Management Solution**

*Simplifying enterprise database backup and recovery*

[![GitHub stars](https://img.shields.io/github/stars/nexwinds/nexpostgres?style=social)](https://github.com/nexwinds/nexpostgres/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/nexwinds/nexpostgres?style=social)](https://github.com/nexwinds/nexpostgres/network/members)

**[🌐 Website](https://nexpostgres.com)** • 
**[📖 Documentation](https://docs.nexpostgres.com)** • 
**[💬 Community](https://github.com/nexwinds/nexpostgres/discussions)** • 
**[🐛 Issues](https://github.com/nexwinds/nexpostgres/issues)**

</div>

### 🏢 Enterprise Support

Need enterprise-grade support or custom features?

- 🎯 **Priority Support** - 24/7 technical assistance
- 🔧 **Custom Development** - Tailored features
- 🎓 **Training & Consulting** - Expert guidance
- 🛡️ **Security Audits** - Comprehensive assessments

**Contact:** [enterprise@nexpostgres.com](mailto:enterprise@nexpostgres.com)

### 🤝 Community

- 💬 **[GitHub Discussions](https://github.com/nexwinds/nexpostgres/discussions)** - Questions and ideas
- 🐛 **[Issue Tracker](https://github.com/nexwinds/nexpostgres/issues)** - Bug reports and features
- 📧 **[Mailing List](https://groups.google.com/g/nexpostgres)** - Announcements

### 📞 Contact

- 📧 **General:** [info@nexpostgres.com](mailto:info@nexpostgres.com)
- 🛠️ **Support:** [support@nexpostgres.com](mailto:support@nexpostgres.com)
- 🔒 **Security:** [security@nexpostgres.com](mailto:security@nexpostgres.com)

---

<div align="center">

**Made with ❤️ by the NEXPOSTGRES Team**

**© 2024 NEXPOSTGRES. All rights reserved.**

</div>