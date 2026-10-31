/**
 * RGW IAM role as returned by the dashboard API (rgwadmin role list/get).
 * Response fields use AWS-style PascalCase (see rgw_rest_role.cc encode_json).
 */
export interface RgwRole {
  RoleId?: string;
  RoleName: string;
  Path?: string;
  Arn?: string;
  CreateDate?: string;
  MaxSessionDuration?: number;
  AssumeRolePolicyDocument?: string;
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

/** Request body for creating a role (snake_case dashboard API). */
export interface RgwRoleCreatePayload {
  role_name: string;
  role_path: string;
  role_assume_policy_doc: string;
  account_id: string;
}

/** Request body for updating a role (snake_case dashboard API). */
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
