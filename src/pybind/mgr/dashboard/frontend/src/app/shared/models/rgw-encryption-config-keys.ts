import { ENCRYPTION_TYPE } from '~/app/ceph/rgw/models/rgw-bucket-encryption';

export enum rgwEncryptionConfigKeys {
  auth = 'Authentication Method',
  encryption_type = 'Encryption Type',
  backend = 'Backend',
  prefix = 'Prefix',
  namespace = 'Namespace',
  secret_engine = 'Secret Engine',
  addr = 'Address',
  token_file = 'Token File',
  ssl_cacert = 'SSL CA Certificate',
  ssl_clientcert = 'SSL Client Certificate',
  ssl_clientkey = 'SSL Client Key',
  verify_ssl = 'Verify SSL',
  ca_path = 'CA Path',
  client_cert = 'Client Certificate',
  client_key = 'Client Key',
  kms_key_template = 'KMS Key Template',
  password = 'Password',
  s3_key_template = 'S3 Key Template',
  username = 'Username'
}

export interface VaultConfig {
  config: {
    addr: string;
    auth: string;
    prefix: string;
    secret_engine: string;
    namespace: string;
    token_file: string;
    ssl_cacert: string;
    ssl_clientcert: string;
    ssl_clientkey: string;
  };
  kms_provider: string;
  encryption_type: ENCRYPTION_TYPE;
}

export interface KmipConfig {
  config: {
    addr: string;
    username: string;
    password: string;
    s3_key_template: string;
    client_cert: string;
    client_key: string;
    ca_path: string;
    kms_key_template: string;
  };
  encryption_type: ENCRYPTION_TYPE;
  kms_provider: string;
}

export interface encryptionDatafromAPI {
  kms: {
    vault: VaultConfig;
    kmip: KmipConfig;
  };
  s3: {
    vault: VaultConfig;
  };
}
