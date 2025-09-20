export enum KMS_PROVIDER {
  VAULT = 'vault',
  KMIP = 'kmip'
}

enum AuthMethods {
  Token = 'token',
  Agent = 'agent'
}

enum SecretEngines {
  KV = 'kv',
  Transit = 'transit'
}

export enum ENCRYPTION_TYPE {
  SSE_S3 = 's3',
  SSE_KMS = 'kms'
}

interface RgwBucketEncryptionModel {
  kmsProviders: KMS_PROVIDER[];
  authMethods: AuthMethods[];
  secretEngines: SecretEngines[];
}

export const rgwBucketEncryptionModel: RgwBucketEncryptionModel = {
  kmsProviders: [KMS_PROVIDER.VAULT, KMS_PROVIDER.KMIP],
  authMethods: [AuthMethods.Token, AuthMethods.Agent],
  secretEngines: [SecretEngines.KV, SecretEngines.Transit]
};
