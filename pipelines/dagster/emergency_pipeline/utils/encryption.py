"""
Encryption Utilities for Emergency Management Pipeline
Federal compliance encryption for tenant data separation and field-level encryption
"""

import os
import base64
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import logging

logger = logging.getLogger(__name__)


class EncryptionKeyManager:
    """Manages encryption keys for the pipeline"""
    
    def __init__(self, key_storage_path: str = "/opt/dagster/storage/keys"):
        self.key_storage_path = key_storage_path
        os.makedirs(key_storage_path, exist_ok=True)
        self.master_key = self._get_or_create_master_key()
    
    def _get_or_create_master_key(self) -> bytes:
        """Get or create master encryption key"""
        master_key_path = os.path.join(self.key_storage_path, "master.key")
        
        if os.path.exists(master_key_path):
            with open(master_key_path, 'rb') as f:
                return f.read()
        else:
            # Generate new master key
            master_key = Fernet.generate_key()
            with open(master_key_path, 'wb') as f:
                f.write(master_key)
            os.chmod(master_key_path, 0o600)  # Restrict permissions
            return master_key
    
    def derive_key(self, identifier: str, salt: bytes = None) -> bytes:
        """Derive encryption key from identifier"""
        if salt is None:
            salt = hashlib.sha256(identifier.encode()).digest()[:16]
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        
        return base64.urlsafe_b64encode(kdf.derive(self.master_key + identifier.encode()))
    
    def generate_tenant_key(self, tenant_id: str) -> bytes:
        """Generate tenant-specific encryption key"""
        return self.derive_key(f"tenant_{tenant_id}")
    
    def generate_field_key(self, field_name: str, tenant_id: str = None) -> bytes:
        """Generate field-specific encryption key"""
        identifier = f"field_{field_name}"
        if tenant_id:
            identifier += f"_tenant_{tenant_id}"
        return self.derive_key(identifier)


class DataEncryption:
    """Main encryption utility for data pipeline"""
    
    def __init__(self, key_manager: EncryptionKeyManager = None):
        self.key_manager = key_manager or EncryptionKeyManager()
        self._cipher_cache: Dict[str, Fernet] = {}
    
    def _get_cipher(self, key: bytes) -> Fernet:
        """Get or create cipher for key (with caching)"""
        key_hash = hashlib.sha256(key).hexdigest()
        
        if key_hash not in self._cipher_cache:
            self._cipher_cache[key_hash] = Fernet(key)
        
        return self._cipher_cache[key_hash]
    
    def encrypt_value(self, value: Any, key: bytes) -> str:
        """Encrypt a single value"""
        if value is None:
            return None
        
        # Convert to string for encryption
        value_str = json.dumps(value) if not isinstance(value, str) else value
        value_bytes = value_str.encode('utf-8')
        
        cipher = self._get_cipher(key)
        encrypted_bytes = cipher.encrypt(value_bytes)
        
        return base64.urlsafe_b64encode(encrypted_bytes).decode('utf-8')
    
    def decrypt_value(self, encrypted_value: str, key: bytes) -> Any:
        """Decrypt a single value"""
        if encrypted_value is None:
            return None
        
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode('utf-8'))
            cipher = self._get_cipher(key)
            decrypted_bytes = cipher.decrypt(encrypted_bytes)
            value_str = decrypted_bytes.decode('utf-8')
            
            # Try to parse as JSON, fall back to string
            try:
                return json.loads(value_str)
            except json.JSONDecodeError:
                return value_str
                
        except Exception as e:
            logger.error(f"Decryption failed: {str(e)}")
            raise
    
    def encrypt_dict(self, data: Dict[str, Any], field_keys: Dict[str, bytes]) -> Dict[str, Any]:
        """Encrypt specific fields in a dictionary"""
        encrypted_data = data.copy()
        
        for field_name, key in field_keys.items():
            if field_name in encrypted_data:
                encrypted_data[field_name] = self.encrypt_value(encrypted_data[field_name], key)
                # Mark field as encrypted
                encrypted_data[f"{field_name}_encrypted"] = True
        
        return encrypted_data
    
    def decrypt_dict(self, encrypted_data: Dict[str, Any], field_keys: Dict[str, bytes]) -> Dict[str, Any]:
        """Decrypt specific fields in a dictionary"""
        decrypted_data = encrypted_data.copy()
        
        for field_name, key in field_keys.items():
            if field_name in decrypted_data and encrypted_data.get(f"{field_name}_encrypted"):
                decrypted_data[field_name] = self.decrypt_value(decrypted_data[field_name], key)
                # Remove encryption marker
                decrypted_data.pop(f"{field_name}_encrypted", None)
        
        return decrypted_data


class TenantKeyManager:
    """Manages encryption keys per tenant for federal compliance"""
    
    def __init__(self, key_manager: EncryptionKeyManager = None):
        self.key_manager = key_manager or EncryptionKeyManager()
        self.tenant_keys: Dict[str, bytes] = {}
        self.field_encryption_config: Dict[str, Dict[str, bool]] = {}
    
    def get_tenant_key(self, tenant_id: str) -> bytes:
        """Get encryption key for tenant"""
        if tenant_id not in self.tenant_keys:
            self.tenant_keys[tenant_id] = self.key_manager.generate_tenant_key(tenant_id)
        return self.tenant_keys[tenant_id]
    
    def configure_field_encryption(self, tenant_id: str, field_config: Dict[str, bool]):
        """Configure which fields should be encrypted for a tenant"""
        self.field_encryption_config[tenant_id] = field_config
    
    def get_encrypted_fields(self, tenant_id: str) -> List[str]:
        """Get list of fields that should be encrypted for tenant"""
        return [
            field for field, should_encrypt 
            in self.field_encryption_config.get(tenant_id, {}).items()
            if should_encrypt
        ]
    
    def encrypt_tenant_data(self, data: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """Encrypt tenant data according to configuration"""
        encrypted_fields = self.get_encrypted_fields(tenant_id)
        
        if not encrypted_fields:
            return data
        
        encryption = DataEncryption(self.key_manager)
        
        # Generate field-specific keys for tenant
        field_keys = {}
        for field in encrypted_fields:
            if field in data:
                field_keys[field] = self.key_manager.generate_field_key(field, tenant_id)
        
        return encryption.encrypt_dict(data, field_keys)
    
    def decrypt_tenant_data(self, encrypted_data: Dict[str, Any], tenant_id: str) -> Dict[str, Any]:
        """Decrypt tenant data"""
        encrypted_fields = self.get_encrypted_fields(tenant_id)
        
        if not encrypted_fields:
            return encrypted_data
        
        encryption = DataEncryption(self.key_manager)
        
        # Generate same field-specific keys for tenant
        field_keys = {}
        for field in encrypted_fields:
            if field in encrypted_data:
                field_keys[field] = self.key_manager.generate_field_key(field, tenant_id)
        
        return encryption.decrypt_dict(encrypted_data, field_keys)


class FieldLevelEncryption:
    """Field-level encryption for sensitive data"""
    
    def __init__(self, key_manager: EncryptionKeyManager = None):
        self.key_manager = key_manager or EncryptionKeyManager()
        self.encryption = DataEncryption(key_manager)
        
        # Define sensitive field patterns
        self.sensitive_patterns = {
            'pii': ['ssn', 'social_security', 'personal_id', 'driver_license'],
            'contact': ['email', 'phone', 'address', 'contact_info'],
            'financial': ['account', 'routing', 'credit_card', 'payment'],
            'location': ['coordinates', 'latitude', 'longitude', 'precise_location']
        }
    
    def identify_sensitive_fields(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Identify sensitive fields in data and categorize them"""
        sensitive_fields = {}
        
        for field_name in data.keys():
            field_lower = field_name.lower()
            
            for category, patterns in self.sensitive_patterns.items():
                if any(pattern in field_lower for pattern in patterns):
                    sensitive_fields[field_name] = category
                    break
        
        return sensitive_fields
    
    def auto_encrypt_sensitive_data(
        self, 
        data: Dict[str, Any], 
        tenant_id: str = None,
        additional_fields: List[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """Automatically encrypt sensitive fields"""
        
        # Identify sensitive fields
        sensitive_fields = self.identify_sensitive_fields(data)
        
        # Add any additional fields specified
        if additional_fields:
            for field in additional_fields:
                if field in data:
                    sensitive_fields[field] = 'custom'
        
        if not sensitive_fields:
            return data, {}
        
        # Generate encryption keys for sensitive fields
        field_keys = {}
        for field_name in sensitive_fields.keys():
            field_keys[field_name] = self.key_manager.generate_field_key(field_name, tenant_id)
        
        # Encrypt the data
        encrypted_data = self.encryption.encrypt_dict(data, field_keys)
        
        return encrypted_data, sensitive_fields
    
    def decrypt_sensitive_data(
        self, 
        encrypted_data: Dict[str, Any], 
        sensitive_fields: Dict[str, str],
        tenant_id: str = None
    ) -> Dict[str, Any]:
        """Decrypt sensitive fields"""
        
        if not sensitive_fields:
            return encrypted_data
        
        # Generate same encryption keys
        field_keys = {}
        for field_name in sensitive_fields.keys():
            field_keys[field_name] = self.key_manager.generate_field_key(field_name, tenant_id)
        
        return self.encryption.decrypt_dict(encrypted_data, field_keys)


class ComplianceEncryption:
    """Encryption utilities for federal compliance requirements"""
    
    def __init__(self, key_manager: EncryptionKeyManager = None):
        self.key_manager = key_manager or EncryptionKeyManager()
        self.tenant_manager = TenantKeyManager(key_manager)
        self.field_encryption = FieldLevelEncryption(key_manager)
    
    def encrypt_for_compliance(
        self, 
        data: Dict[str, Any], 
        tenant_id: str,
        classification_level: str = "INTERNAL"
    ) -> Dict[str, Any]:
        """Encrypt data according to compliance requirements"""
        
        # Add compliance metadata
        compliance_data = data.copy()
        compliance_data.update({
            'encryption_timestamp': datetime.now().isoformat(),
            'classification_level': classification_level,
            'tenant_id': tenant_id,
            'encryption_version': '1.0'
        })
        
        # Apply different encryption based on classification level
        if classification_level in ['RESTRICTED', 'CONFIDENTIAL']:
            # High-security: encrypt all sensitive fields + additional fields
            additional_fields = ['organization', 'contact_person', 'internal_notes']
            encrypted_data, sensitive_fields = self.field_encryption.auto_encrypt_sensitive_data(
                compliance_data, tenant_id, additional_fields
            )
            
            # Store encryption metadata
            encrypted_data['encrypted_fields'] = list(sensitive_fields.keys())
            encrypted_data['encryption_level'] = 'high'
            
        elif classification_level == 'INTERNAL':
            # Medium-security: encrypt PII and contact info
            encrypted_data, sensitive_fields = self.field_encryption.auto_encrypt_sensitive_data(
                compliance_data, tenant_id
            )
            
            encrypted_data['encrypted_fields'] = list(sensitive_fields.keys())
            encrypted_data['encryption_level'] = 'medium'
            
        else:  # PUBLIC
            # Low-security: only encrypt if explicitly required
            encrypted_data = compliance_data
            encrypted_data['encryption_level'] = 'none'
        
        return encrypted_data
    
    def decrypt_for_access(
        self, 
        encrypted_data: Dict[str, Any], 
        tenant_id: str,
        user_clearance_level: str = "INTERNAL"
    ) -> Dict[str, Any]:
        """Decrypt data based on user access level"""
        
        data_classification = encrypted_data.get('classification_level', 'PUBLIC')
        encryption_level = encrypted_data.get('encryption_level', 'none')
        
        # Check if user has sufficient clearance
        clearance_hierarchy = ['PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL']
        
        user_level_index = clearance_hierarchy.index(user_clearance_level) if user_clearance_level in clearance_hierarchy else 0
        data_level_index = clearance_hierarchy.index(data_classification) if data_classification in clearance_hierarchy else 0
        
        if user_level_index < data_level_index:
            # User doesn't have sufficient clearance
            logger.warning(f"User clearance {user_clearance_level} insufficient for {data_classification} data")
            return self._create_redacted_data(encrypted_data)
        
        # User has sufficient clearance, decrypt data
        if encryption_level in ['medium', 'high']:
            encrypted_fields = encrypted_data.get('encrypted_fields', [])
            sensitive_fields = {field: 'encrypted' for field in encrypted_fields}
            
            decrypted_data = self.field_encryption.decrypt_sensitive_data(
                encrypted_data, sensitive_fields, tenant_id
            )
        else:
            decrypted_data = encrypted_data
        
        # Remove encryption metadata from final output
        metadata_fields = ['encryption_timestamp', 'encrypted_fields', 'encryption_level', 'encryption_version']
        for field in metadata_fields:
            decrypted_data.pop(field, None)
        
        return decrypted_data
    
    def _create_redacted_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create redacted version of data for insufficient clearance"""
        redacted_data = {}
        
        # Only include non-sensitive fields
        safe_fields = [
            'id', 'timestamp', 'data_source', 'state', 'public_status',
            'classification_level', 'data_classification'
        ]
        
        for field in safe_fields:
            if field in data:
                redacted_data[field] = data[field]
        
        redacted_data['access_note'] = 'Data redacted due to insufficient clearance level'
        
        return redacted_data
    
    def audit_encryption_access(
        self, 
        data_id: str, 
        tenant_id: str, 
        user_id: str, 
        access_type: str,
        success: bool
    ):
        """Audit encryption-related access for compliance"""
        
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'data_id': data_id,
            'tenant_id': tenant_id,
            'user_id': user_id,
            'access_type': access_type,  # 'encrypt', 'decrypt', 'view'
            'success': success,
            'compliance_event': True
        }
        
        # In production, this would write to a secure audit log
        audit_log_path = os.path.join(self.key_manager.key_storage_path, "encryption_audit.log")
        
        try:
            with open(audit_log_path, 'a') as f:
                f.write(json.dumps(audit_entry) + '\n')
        except Exception as e:
            logger.error(f"Failed to write encryption audit log: {str(e)}")


class EncryptionRotationManager:
    """Manages encryption key rotation for security"""
    
    def __init__(self, key_manager: EncryptionKeyManager):
        self.key_manager = key_manager
        self.rotation_schedule = {}  # tenant_id -> next_rotation_date
    
    def schedule_key_rotation(self, tenant_id: str, rotation_interval_days: int = 90):
        """Schedule key rotation for tenant"""
        next_rotation = datetime.now() + timedelta(days=rotation_interval_days)
        self.rotation_schedule[tenant_id] = next_rotation
    
    def check_rotation_needed(self, tenant_id: str) -> bool:
        """Check if key rotation is needed for tenant"""
        if tenant_id not in self.rotation_schedule:
            return False
        
        return datetime.now() >= self.rotation_schedule[tenant_id]
    
    def rotate_tenant_keys(self, tenant_id: str) -> bool:
        """Rotate encryption keys for tenant"""
        try:
            # Generate new key
            new_key = self.key_manager.generate_tenant_key(f"{tenant_id}_rotated_{datetime.now().isoformat()}")
            
            # Store old key for decryption of historical data
            old_key_path = os.path.join(
                self.key_manager.key_storage_path, 
                f"{tenant_id}_old_{datetime.now().strftime('%Y%m%d')}.key"
            )
            
            current_key = self.key_manager.generate_tenant_key(tenant_id)
            with open(old_key_path, 'wb') as f:
                f.write(current_key)
            
            # Update rotation schedule
            self.schedule_key_rotation(tenant_id)
            
            logger.info(f"Successfully rotated encryption keys for tenant {tenant_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rotate keys for tenant {tenant_id}: {str(e)}")
            return False