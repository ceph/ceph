export enum RgwUserAvailableCapability {
  USERS = 'users',
  BUCKETS = 'buckets',
  METADATA = 'metadata',
  USAGE = 'usage',
  ZONE = 'zone'
}

export class RgwUserCapabilities {
  static readonly capabilities = RgwUserAvailableCapability;

  static getAll(): string[] {
    return Object.values(RgwUserCapabilities.capabilities);
  }
}
