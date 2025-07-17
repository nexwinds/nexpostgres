import paramiko
import os
import tempfile
import logging

class SSHManager:
    def __init__(self, host, port, username, ssh_key_content=None):
        self.host = host
        self.port = port
        self.username = username
        self.ssh_key_content = ssh_key_content
        self.client = None
        self.temp_key_path = None
        self.logger = logging.getLogger('nexpostgres.ssh')

    def connect(self):
        """Establish an SSH connection to the remote server"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Create temporary file for SSH key content
            temp_key_file = None
            pkey = None
            if self.ssh_key_content:
                # Normalize line endings and ensure proper format
                key_content = self.ssh_key_content.strip().replace('\r\n', '\n').replace('\r', '\n')
                
                # Ensure key ends with newline
                if not key_content.endswith('\n'):
                    key_content += '\n'
                
                temp_key_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                temp_key_file.write(key_content)
                temp_key_file.close()
                self.temp_key_path = temp_key_file.name
                
                print(f"DEBUG SSH: Created temp key file: {self.temp_key_path}")
                print(f"DEBUG SSH: SSH key content length: {len(key_content)}")
                print(f"DEBUG SSH: SSH key starts with: {key_content[:50]}...")
                print(f"DEBUG SSH: SSH key ends with: ...{key_content[-50:]}")
                
                # Try to load the key directly with paramiko to validate format
                try:
                    from io import StringIO
                    key_file_obj = StringIO(key_content)
                    
                    # Try different key types
                    key_file_obj.seek(0)
                    if 'BEGIN OPENSSH PRIVATE KEY' in key_content:
                        pkey = paramiko.Ed25519Key.from_private_key(key_file_obj)
                        print(f"DEBUG SSH: Successfully loaded as Ed25519 key")
                    elif 'BEGIN RSA PRIVATE KEY' in key_content:
                        pkey = paramiko.RSAKey.from_private_key(key_file_obj)
                        print(f"DEBUG SSH: Successfully loaded as RSA key")
                    elif 'BEGIN EC PRIVATE KEY' in key_content:
                        pkey = paramiko.ECDSAKey.from_private_key(key_file_obj)
                        print(f"DEBUG SSH: Successfully loaded as ECDSA key")
                    else:
                        print(f"DEBUG SSH: Unknown key format, trying generic loading")
                        
                except Exception as key_error:
                    print(f"DEBUG SSH: Failed to load key directly: {str(key_error)}")
                    pkey = None
            
            print(f"DEBUG SSH: Attempting connection to {self.username}@{self.host}:{self.port}")
            print(f"DEBUG SSH: Using key file: {self.temp_key_path if self.temp_key_path else 'None'}")
            print(f"DEBUG SSH: Loaded pkey object: {pkey is not None}")
            
            # Try connection with pkey first, then fallback to key file
            if pkey:
                print(f"DEBUG SSH: Attempting connection with pkey object")
                self.client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    pkey=pkey
                )
            else:
                print(f"DEBUG SSH: Attempting connection with key file")
                self.client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    key_filename=self.temp_key_path if self.temp_key_path else None
                )
            
            # Clean up temp file after connection is established
            if temp_key_file and self.temp_key_path:
                os.unlink(self.temp_key_path)
                self.temp_key_path = None
            
            self.logger.info(f"Connected to {self.username}@{self.host}:{self.port}")
            print(f"DEBUG SSH: Connection successful to {self.username}@{self.host}:{self.port}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {self.username}@{self.host}:{self.port}: {str(e)}")
            print(f"DEBUG SSH: Connection failed to {self.username}@{self.host}:{self.port}: {str(e)}")
            
            # Clean up temp file in case of error
            if temp_key_file and self.temp_key_path:
                try:
                    os.unlink(self.temp_key_path)
                except:
                    pass
                self.temp_key_path = None
                
            return False

    def disconnect(self):
        """Close the SSH connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.logger.info(f"Disconnected from {self.username}@{self.host}:{self.port}")

    def execute_command(self, command):
        """Execute a command on the remote server"""
        if not self.client:
            raise ConnectionError("Not connected to SSH server. Call connect() first.")
        
        try:
            self.logger.debug(f"Executing command: {command}")
            stdin, stdout, stderr = self.client.exec_command(command)
            
            exit_code = stdout.channel.recv_exit_status()
            stdout_content = stdout.read().decode('utf-8', errors='replace')
            stderr_content = stderr.read().decode('utf-8', errors='replace')
            
            if stderr_content:
                self.logger.debug(f"Command stderr: {stderr_content}")
                
            return {
                'exit_code': exit_code,
                'stdout': stdout_content,
                'stderr': stderr_content
            }
        except Exception as e:
            self.logger.error(f"Error executing command '{command}': {str(e)}")
            return {
                'exit_code': -1,
                'stdout': '',
                'stderr': str(e)
            }

    def check_path_exists(self, path, is_dir=False):
        """Check if a file or directory exists on the remote server"""
        test_type = "-d" if is_dir else "-f"
        result = self.execute_command(f"test {test_type} {path} && echo 'EXISTS' || echo 'NOT_EXISTS'")
        return result['stdout'].strip() == 'EXISTS'
        
    def check_file_exists(self, path):
        """Check if a file exists on the remote server"""
        return self.check_path_exists(path, is_dir=False)
    
    def check_directory_exists(self, path):
        """Check if a directory exists on the remote server"""
        return self.check_path_exists(path, is_dir=True)
    
    def transfer_file(self, local_path, remote_path, direction="upload"):
        """Transfer a file to or from the remote server"""
        try:
            sftp = self.client.open_sftp()
            
            if direction == "upload":
                sftp.put(local_path, remote_path)
                self.logger.info(f"Uploaded {local_path} to {remote_path}")
            else:
                sftp.get(remote_path, local_path)
                self.logger.info(f"Downloaded {remote_path} to {local_path}")
                
            sftp.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to {direction} {local_path} <-> {remote_path}: {str(e)}")
            return False
            
    def upload_file(self, local_path, remote_path):
        """Upload a file to the remote server"""
        return self.transfer_file(local_path, remote_path, direction="upload")
    
    def download_file(self, remote_path, local_path):
        """Download a file from the remote server"""
        return self.transfer_file(local_path, remote_path, direction="download")
            
    def write_file_content(self, remote_path, content):
        """Write content to a file on the remote server"""
        try:
            sftp = self.client.open_sftp()
            with sftp.file(remote_path, 'w') as f:
                f.write(content)
            sftp.close()
            self.logger.info(f"Wrote content to {remote_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write content to {remote_path}: {str(e)}")
            return False
            
    def read_file_content(self, remote_path):
        """Read content from a file on the remote server"""
        try:
            sftp = self.client.open_sftp()
            with sftp.file(remote_path, 'r') as f:
                content = f.read()
            sftp.close()
            return content.decode('utf-8', errors='replace')
        except Exception as e:
            self.logger.error(f"Failed to read content from {remote_path}: {str(e)}")
            return None

def test_ssh_connection(host, port, username, ssh_key_content):
    """Test SSH connection to a remote server"""
    ssh = SSHManager(host, port, username, ssh_key_content)
    connected = ssh.connect()
    if connected:
        ssh.disconnect()
    return connected