// NEXPOSTGRES JavaScript functions

document.addEventListener('DOMContentLoaded', function() {
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
    
    // SSH Key method toggle
    const sshKeyMethodRadios = document.querySelectorAll('input[name="ssh_key_method"]');
    const sshKeyPathDiv = document.getElementById('ssh_key_path_div');
    const sshKeyContentDiv = document.getElementById('ssh_key_content_div');
    
    if (sshKeyMethodRadios.length > 0 && sshKeyPathDiv && sshKeyContentDiv) {
        sshKeyMethodRadios.forEach(function(radio) {
            radio.addEventListener('change', function() {
                if (this.value === 'path') {
                    sshKeyPathDiv.style.display = 'block';
                    sshKeyContentDiv.style.display = 'none';
                } else if (this.value === 'content') {
                    sshKeyPathDiv.style.display = 'none';
                    sshKeyContentDiv.style.display = 'block';
                }
            });
        });
        
        // Trigger change event on page load
        const checkedRadio = document.querySelector('input[name="ssh_key_method"]:checked');
        if (checkedRadio) {
            checkedRadio.dispatchEvent(new Event('change'));
        }
    }
    
    // Cron expression builder
    setupCronExpressionBuilder();
    
    // Test S3 connection
    const testS3Button = document.getElementById('test-s3-connection');
    if (testS3Button) {
        testS3Button.addEventListener('click', testS3Connection);
    }
    
    // Restore point-in-time toggle
    const usePitrCheckbox = document.getElementById('use_pitr');
    const pitrControls = document.getElementById('pitr_controls');
    
    if (usePitrCheckbox && pitrControls) {
        usePitrCheckbox.addEventListener('change', function() {
            pitrControls.style.display = this.checked ? 'block' : 'none';
        });
        
        // Trigger change event on page load
        usePitrCheckbox.dispatchEvent(new Event('change'));
    }
    
    // Test SSH connection
    const testSshButton = document.getElementById('test-ssh-connection');
    if (testSshButton) {
        testSshButton.addEventListener('click', testSshConnection);
    }
});

// Cron expression builder
function setupCronExpressionBuilder() {
    const cronBuilder = document.getElementById('cron-builder');
    const cronExpressionInput = document.getElementById('cron_expression');
    
    if (!cronBuilder || !cronExpressionInput) return;
    
    const minuteSelect = document.getElementById('cron_minute');
    const hourSelect = document.getElementById('cron_hour');
    const dayOfMonthSelect = document.getElementById('cron_day_of_month');
    const monthSelect = document.getElementById('cron_month');
    const dayOfWeekSelect = document.getElementById('cron_day_of_week');
    
    const updateCronExpression = function() {
        const minute = minuteSelect.value;
        const hour = hourSelect.value;
        const dayOfMonth = dayOfMonthSelect.value;
        const month = monthSelect.value;
        const dayOfWeek = dayOfWeekSelect.value;
        
        cronExpressionInput.value = `${minute} ${hour} ${dayOfMonth} ${month} ${dayOfWeek}`;
    };
    
    // Add event listeners to all select elements
    [minuteSelect, hourSelect, dayOfMonthSelect, monthSelect, dayOfWeekSelect].forEach(function(select) {
        if (select) {
            select.addEventListener('change', updateCronExpression);
        }
    });
    
    // If the cron expression input already has a value, populate the selects accordingly
    if (cronExpressionInput.value) {
        const parts = cronExpressionInput.value.trim().split(/\s+/);
        if (parts.length === 5) {
            if (minuteSelect) minuteSelect.value = parts[0];
            if (hourSelect) hourSelect.value = parts[1];
            if (dayOfMonthSelect) dayOfMonthSelect.value = parts[2];
            if (monthSelect) monthSelect.value = parts[3];
            if (dayOfWeekSelect) dayOfWeekSelect.value = parts[4];
        }
    } else {
        // Set a default cron expression (every day at midnight)
        if (minuteSelect) minuteSelect.value = '0';
        if (hourSelect) hourSelect.value = '0';
        if (dayOfMonthSelect) dayOfMonthSelect.value = '*';
        if (monthSelect) monthSelect.value = '*';
        if (dayOfWeekSelect) dayOfWeekSelect.value = '*';
        
        updateCronExpression();
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
    const sshKeyMethod = document.querySelector('input[name="ssh_key_method"]:checked').value;
    
    let sshKeyPath = '';
    let sshKeyContent = '';
    
    if (sshKeyMethod === 'path') {
        sshKeyPath = document.getElementById('ssh_key_path').value;
        if (!sshKeyPath) {
            alert('Please enter an SSH key path');
            return;
        }
    } else if (sshKeyMethod === 'content') {
        sshKeyContent = document.getElementById('ssh_key_content').value;
        if (!sshKeyContent) {
            alert('Please enter SSH key content');
            return;
        }
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
            'ssh_key_method': sshKeyMethod,
            'ssh_key_path': sshKeyPath,
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