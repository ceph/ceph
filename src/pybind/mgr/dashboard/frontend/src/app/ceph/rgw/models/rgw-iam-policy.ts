export interface IamPolicy {
  PolicyName: string;
  PolicyId?: string;
  Arn: string;
  Path?: string;
  DefaultVersionId?: string;
  AttachmentCount?: number;
  CreateDate?: string;
  UpdateDate?: string;
  Description?: string;
  PolicyDocument?: string | Record<string, unknown>;
}

export interface IamPolicyVersion {
  VersionId: string;
  IsDefaultVersion?: boolean | string;
  CreateDate?: string;
  Document?: string | Record<string, unknown>;
}

export interface IamPolicyTag {
  Key: string;
  Value: string;
}

export interface IamPolicyCreatePayload {
  policy_name: string;
  policy_doc: string;
  path?: string;
  description?: string;
}

export const DEFAULT_IAM_POLICY_DOCUMENT = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": "*"
    }
  ]
}`;
