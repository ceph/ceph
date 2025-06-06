from enum import Enum
from typing import List, NamedTuple, Optional


class VaultConfig(NamedTuple):
    addr: str
    auth: str
    prefix: str
    secret_engine: str
    namespace: Optional[str] = None
    token_file: Optional[str] = None
    ssl_cacert: Optional[str] = None
    ssl_clientcert: Optional[str] = None
    ssl_clientkey: Optional[str] = None
    verify_ssl: Optional[bool] = False
    backend: Optional[str] = None
    encryption_type: Optional[str] = None
    unique_id: Optional[str] = None

    @classmethod
    def required_fields(cls):
        return [field for field in cls._fields if field not in cls._field_defaults]

    @classmethod
    def ceph_config_fields(cls):
        return [field for field in cls._fields if
                field not in ['backend', 'encryption_type', 'unique_id']]


class KmipConfig(NamedTuple):
    addr: str
    username: Optional[str] = None
    password: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    ca_path: Optional[str] = None
    kms_key_template: Optional[str] = None
    s3_key_template: Optional[str] = None
    backend: Optional[str] = None
    encryption_type: Optional[str] = None
    unique_id: Optional[str] = None

    @classmethod
    def required_fields(cls):
        return [field for field in cls._fields if field not in cls._field_defaults]

    @classmethod
    def ceph_config_fields(cls):
        return [field for field in cls._fields if
                field not in ['backend', 'encryption_type', 'unique_id']]


class KmsProviders(Enum):
    VAULT = 'vault'
    KMIP = 'kmip'


class EncryptionTypes(Enum):
    KMS = 'kms'
    S3 = 's3'


class KmsConfig(NamedTuple):
    vault: Optional[VaultConfig] = None
    kmip: Optional[KmipConfig] = None


class S3Config(NamedTuple):
    vault: VaultConfig


class EncryptionConfig(NamedTuple):
    kms: Optional[List[KmsConfig]] = None
    s3: Optional[List[S3Config]] = None

    def to_dict(self):
        """
        Converts the EncryptionConfig class to a dictionary, ensuring that
        'kms' and 's3' entries are stored as dictionaries rather than lists.
        """

        def convert_namedtuple(obj):
            if isinstance(obj, tuple) and hasattr(obj, '_fields'):
                return {field: convert_namedtuple(getattr(obj, field))
                        for field in obj._fields if getattr(obj, field) is not None}
            elif isinstance(obj, list):
                if all(isinstance(item, tuple) and hasattr(item, '_fields') for item in obj):
                    return {key: convert_namedtuple(value) for entry in obj
                            for key, value in convert_namedtuple(entry).items()}
                return [convert_namedtuple(item) for item in obj]
            elif isinstance(obj, dict):
                return {key: convert_namedtuple(value)
                        for key, value in obj.items() if value is not None}
            else:
                return obj

        return {
            "kms": convert_namedtuple(self.kms) if self.kms else [],
            "s3": convert_namedtuple(self.s3) if self.s3 else []
        }
