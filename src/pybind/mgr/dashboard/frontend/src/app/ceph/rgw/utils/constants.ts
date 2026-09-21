export const RGW = 'rgw';

export enum ManagedPolicyName {
  AmazonS3FullAccess = 'AmazonS3FullAccess',
  AmazonS3ReadOnlyAccess = 'AmazonS3ReadOnlyAccess'
}

export const ManagedPolicyArnMap: Record<ManagedPolicyName, string> = {
  [ManagedPolicyName.AmazonS3FullAccess]: 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
  [ManagedPolicyName.AmazonS3ReadOnlyAccess]: 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
};

export interface PredefinedPolicyTemplate {
  name: string;
  description: string;
  policy_doc: string;
}

export const PREDEFINED_POLICY_TEMPLATES: PredefinedPolicyTemplate[] = [
  {
    name: 'AmazonS3ReadOnlyAccess',
    description: 'Provides read-only access to all S3 buckets and objects',
    policy_doc: JSON.stringify(
      {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Action: ['s3:Get*', 's3:List*'],
            Resource: '*'
          }
        ]
      },
      null,
      2
    )
  },
  {
    name: 'AmazonS3FullAccess',
    description: 'Provides full access to all S3 buckets and objects',
    policy_doc: JSON.stringify(
      {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Action: ['s3:*'],
            Resource: '*'
          }
        ]
      },
      null,
      2
    )
  },
  {
    name: 'AmazonS3PutObjectOnly',
    description: 'Allows uploading objects only without read or delete permissions',
    policy_doc: JSON.stringify(
      {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Action: ['s3:PutObject'],
            Resource: '*'
          }
        ]
      },
      null,
      2
    )
  }
];
