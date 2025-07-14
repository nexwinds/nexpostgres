/**
 * Common JavaScript utilities for NEXDB application
 * This file contains reusable functions to reduce code duplication across templates
 */

/**
 * Generate a random password with specified length and character sets
 * @param {number} length - Password length (default: 12)
 * @param {boolean} includeSpecial - Include special characters (default: true)
 * @returns {string} Generated password
 */
function generatePassword(length = 12, includeSpecial = true) {
    const lowercase = 'abcdefghijklmnopqrstuvwxyz';
    const uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const numbers = '0123456789';
    const special = '!@#$%^&*()_+-=[]{}|;:,.<>?';
    
    let charset = lowercase + uppercase + numbers;
    if (includeSpecial) {
        charset += special;
    }
    
    let password = '';
    for (let i = 0; i < length; i++) {
        password += charset.charAt(Math.floor(Math.random() * charset.length));
    }
    
    return password;
}

/**
 * Update username field based on database name
 * @param {string} databaseNameId - ID of database name input field
 * @param {string} usernameId - ID of username input field
 */
function updateUsername(databaseNameId = 'name', usernameId = 'username') {
    const databaseName = document.getElementById(databaseNameId)?.value || '';
    const usernameField = document.getElementById(usernameId);
    
    if (usernameField && databaseName) {
        // Generate username from database name (remove special chars, limit length)
        const username = databaseName
            .toLowerCase()
            .replace(/[^a-z0-9]/g, '')
            .substring(0, 20);
        usernameField.value = username;
    }
}

/**
 * Copy text to clipboard
 * @param {string} elementId - ID of element containing text to copy
 * @param {string} successMessage - Success message to show (optional)
 */
function copyToClipboard(elementId, successMessage = 'Copied to clipboard!') {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const text = element.textContent || element.value;
    
    if (navigator.clipboard && window.isSecureContext) {
        // Use modern clipboard API
        navigator.clipboard.writeText(text).then(() => {
            showToast(successMessage, 'success');
        }).catch(err => {
            console.error('Failed to copy text: ', err);
            fallbackCopyToClipboard(text, successMessage);
        });
    } else {
        // Fallback for older browsers
        fallbackCopyToClipboard(text, successMessage);
    }
}

/**
 * Fallback method for copying to clipboard
 * @param {string} text - Text to copy
 * @param {string} successMessage - Success message to show
 */
function fallbackCopyToClipboard(text, successMessage) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        document.execCommand('copy');
        showToast(successMessage, 'success');
    } catch (err) {
        console.error('Fallback: Oops, unable to copy', err);
        showToast('Failed to copy to clipboard', 'error');
    }
    
    document.body.removeChild(textArea);
}

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Toast type ('success', 'error', 'warning', 'info')
 * @param {number} duration - Duration in milliseconds (default: 3000)
 */
function showToast(message, type = 'info', duration = 3000) {
    // Create toast element
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 12px 20px;
        border-radius: 4px;
        color: white;
        font-weight: 500;
        z-index: 9999;
        opacity: 0;
        transition: opacity 0.3s ease;
    `;
    
    // Set background color based on type
    const colors = {
        success: '#28a745',
        error: '#dc3545',
        warning: '#ffc107',
        info: '#17a2b8'
    };
    toast.style.backgroundColor = colors[type] || colors.info;
    
    toast.textContent = message;
    document.body.appendChild(toast);
    
    // Show toast
    setTimeout(() => {
        toast.style.opacity = '1';
    }, 100);
    
    // Hide and remove toast
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, duration);
}

/**
 * Set progress bar color based on percentage
 * @param {number} percent - Percentage value (0-100)
 * @returns {string} Bootstrap color class
 */
function getProgressColor(percent) {
    if (percent >= 90) return 'bg-danger';
    if (percent >= 75) return 'bg-warning';
    if (percent >= 50) return 'bg-info';
    return 'bg-success';
}

/**
 * Format bytes to human readable format
 * @param {number} bytes - Number of bytes
 * @param {number} decimals - Number of decimal places (default: 2)
 * @returns {string} Formatted string
 */
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Debounce function to limit function calls
 * @param {Function} func - Function to debounce
 * @param {number} wait - Wait time in milliseconds
 * @param {boolean} immediate - Execute immediately on first call
 * @returns {Function} Debounced function
 */
function debounce(func, wait, immediate) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

/**
 * Show/hide loading spinner on button
 * @param {string} buttonId - ID of button element
 * @param {boolean} loading - Whether to show loading state
 * @param {string} loadingText - Text to show during loading (optional)
 */
function toggleButtonLoading(buttonId, loading, loadingText = 'Loading...') {
    const button = document.getElementById(buttonId);
    if (!button) return;
    
    if (loading) {
        button.dataset.originalText = button.textContent;
        button.innerHTML = `<span class="spinner-border spinner-border-sm me-2" role="status"></span>${loadingText}`;
        button.disabled = true;
    } else {
        button.textContent = button.dataset.originalText || button.textContent;
        button.disabled = false;
    }
}

// Auto-initialize common functionality when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Auto-setup password generation buttons
    const generatePasswordBtns = document.querySelectorAll('[data-generate-password]');
    generatePasswordBtns.forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.dataset.generatePassword || 'password';
            const targetField = document.getElementById(targetId);
            if (targetField) {
                targetField.value = generatePassword();
            }
        });
    });
    
    // Auto-setup copy to clipboard buttons
    const copyBtns = document.querySelectorAll('[data-copy-target]');
    copyBtns.forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.dataset.copyTarget;
            copyToClipboard(targetId);
        });
    });
});