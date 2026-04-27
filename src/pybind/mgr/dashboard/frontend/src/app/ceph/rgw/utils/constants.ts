export const RGW = 'rgw';

export enum ManagedPolicyName {
  AmazonS3FullAccess = 'AmazonS3FullAccess',
  AmazonS3ReadOnlyAccess = 'AmazonS3ReadOnlyAccess'
}

export const ManagedPolicyArnMap: Record<ManagedPolicyName, string> = {
  [ManagedPolicyName.AmazonS3FullAccess]: 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
  [ManagedPolicyName.AmazonS3ReadOnlyAccess]: 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
};
