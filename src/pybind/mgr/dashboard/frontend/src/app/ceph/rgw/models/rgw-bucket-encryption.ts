enum KmsProviders {
  Vault = 'vault'
}

enum AuthMethods {
  Token = 'token',
  Agent = 'agent'
}

enum SecretEngines {
  KV = 'kv',
  Transit = 'transit'
}

enum sseS3 {
  SSE_S3 = 'AES256'
}

enum sseKms {
  SSE_KMS = 'aws:kms'
}

interface RgwBucketEncryptionModel {
  kmsProviders: KmsProviders[];
  authMethods: AuthMethods[];
  secretEngines: SecretEngines[];
  SSE_S3: sseS3;
  SSE_KMS: sseKms;
}

export const rgwBucketEncryptionModel: RgwBucketEncryptionModel = {
  kmsProviders: [KmsProviders.Vault],
  authMethods: [AuthMethods.Token, AuthMethods.Agent],
  secretEngines: [SecretEngines.KV, SecretEngines.Transit],
  SSE_S3: sseS3.SSE_S3,
  SSE_KMS: sseKms.SSE_KMS
};
