export interface RgwRole {
  RoleId: string;
  RoleName: string;
  Path: string;
  Arn: string;
  CreateDate: string;
  MaxSessionDuration: number;
  AssumeRolePolicyDocument: string;
  policies_count?: number;
  PermissionPolicies?: string[];
}

export interface RgwPolicyStatement {
  Effect?: string;
  Action?: string | string[];
  Resource?: string | string[];
  Principal?: string | Record<string, string[]>;
}

export interface RgwPolicyDocument {
  Version?: string;
  Statement?: RgwPolicyStatement[];
}

export interface RgwRolePolicyResponse {
  PolicyName?: string;
  PolicyDocument?: string | RgwPolicyDocument;
  'Permission policy'?: string | RgwPolicyDocument;
}

export interface RgwRoleCreatePayload {
  role_name: string;
  role_path: string;
  role_assume_policy_doc: string;
  account_id: string;
}

export interface RgwRoleUpdatePayload {
  role_name: string;
  max_session_duration: number;
  account_id: string;
}

export interface RgwRolePolicy {
  name: string;
}

export interface RgwRolePoliciesCountCellContext {
  data: {
    value: number;
    row: RgwRole;
  };
}
