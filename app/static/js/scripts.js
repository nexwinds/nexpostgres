// NEXPOSTGRES JavaScript functions

document.addEventListener('DOMContentLoaded', function() {
    // Initialize theme
    initializeTheme();
    
    // Theme toggle
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }
    
    // Auto-close alerts after 5 seconds
    setTimeout(function() {
        const alerts = document.querySelectorAll('.alert');
        alerts.forEach(function(alert) {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        });
    }, 5000);
    
    // Initialize tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
    
    // Test S3 connection
    const testS3Button = document.getElementById('test-s3-connection');
    if (testS3Button) {
        testS3Button.addEventListener('click', testS3Connection);
    }
    
    // Test SSH connection
    const testSshButton = document.getElementById('test-ssh-connection');
    if (testSshButton) {
        testSshButton.addEventListener('click', testSshConnection);
    }
});

// Initialize theme based on localStorage or system preference
function initializeTheme() {
    const savedTheme = localStorage.getItem('theme');
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const defaultTheme = savedTheme || (prefersDark ? 'dark' : 'light');
    
    setTheme(defaultTheme);
}

// Toggle between light and dark themes
function toggleTheme() {
    const htmlElement = document.documentElement;
    const currentTheme = htmlElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    
    setTheme(newTheme);
    localStorage.setItem('theme', newTheme);
}

// Set the theme
function setTheme(theme) {
    const htmlElement = document.documentElement;
    const themeIcon = document.getElementById('theme-icon');
    
    htmlElement.setAttribute('data-theme', theme);
    
    if (themeIcon) {
        themeIcon.className = theme === 'dark' ? 'fas fa-moon me-1' : 'fas fa-sun me-1';
    }
    
    // Remove bg-light class from body if dark theme
    if (theme === 'dark') {
        document.body.classList.remove('bg-light');
    } else {
        if (!document.body.classList.contains('bg-light')) {
            document.body.classList.add('bg-light');
        }
    }
}

// Test S3 connection
function testS3Connection() {
    const s3Bucket = document.getElementById('s3_bucket').value;
    const s3Region = document.getElementById('s3_region').value;
    const s3AccessKey = document.getElementById('s3_access_key').value;
    const s3SecretKey = document.getElementById('s3_secret_key').value;
    
    const connectionStatus = document.getElementById('s3-connection-status');
    
    if (!s3Bucket || !s3Region || !s3AccessKey || !s3SecretKey) {
        connectionStatus.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i> Please fill in all S3 fields';
        connectionStatus.className = 'connection-status connection-failure';
        connectionStatus.style.display = 'block';
        return;
    }
    
    // Show loading indicator
    connectionStatus.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i> Testing connection...';
    connectionStatus.className = 'connection-status';
    connectionStatus.style.display = 'block';
    
    // Send request to test S3 connection
    fetch('/backups/test-s3', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
            's3_bucket': s3Bucket,
            's3_region': s3Region,
            's3_access_key': s3AccessKey,
            's3_secret_key': s3SecretKey
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            connectionStatus.innerHTML = '<i class="fas fa-check-circle me-2"></i> ' + data.message;
            connectionStatus.className = 'connection-status connection-success';
        } else {
            connectionStatus.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i> ' + data.message;
            connectionStatus.className = 'connection-status connection-failure';
        }
        connectionStatus.style.display = 'block';
    })
    .catch(error => {
        connectionStatus.innerHTML = '<i class="fas fa-exclamation-circle me-2"></i> Connection test failed: ' + error.message;
        connectionStatus.className = 'connection-status connection-failure';
        connectionStatus.style.display = 'block';
    });
}

// Test SSH connection
function testSshConnection() {
    const host = document.getElementById('host').value;
    const port = document.getElementById('port').value;
    const username = document.getElementById('username').value;
    let sshKeyContent = document.getElementById('ssh_key_content').value;
    
    if (!sshKeyContent) {
        alert('Please enter SSH key content');
        return;
    }
    
    if (!host || !username) {
        alert('Please fill in all required fields');
        return;
    }
    
    // Show loading indicator
    const testButton = document.getElementById('test-ssh-connection');
    const originalText = testButton.innerHTML;
    testButton.disabled = true;
    testButton.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i> Testing...';
    
    // Send request to test SSH connection
    fetch('/servers/test-connection', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
            'host': host,
            'port': port,
            'username': username,
            'ssh_key_content': sshKeyContent
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            alert('Connection successful!');
        } else {
            alert('Connection failed: ' + data.message);
        }
    })
    .catch(error => {
        alert('Connection test failed: ' + error.message);
    })
    .finally(() => {
        // Restore button
        testButton.disabled = false;
        testButton.innerHTML = originalText;
    });
}