export enum RgwBucketAclPermissions {
  Read = 'Read',
  Write = 'Write',
  All = 'Read and write',
  FullControl = 'Full control'
}

export enum RgwBucketAclGrantee {
  Owner = 'Owner',
  Everyone = 'Everyone',
  AuthenticatedUsers = 'Authenticated Users'
}

export type AclPermissionsType = RgwBucketAclPermissions[keyof RgwBucketAclPermissions];
