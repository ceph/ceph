export class RgwBucketEncryptionModel {
  kmsProviders = ['vault'];
  authMethods = ['token', 'agent'];
  secretEngines = ['kv', 'transit'];
  sse_s3 = 'AES256';
  sse_kms = 'aws:kms';
}
