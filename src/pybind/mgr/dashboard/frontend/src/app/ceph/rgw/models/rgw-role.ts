export interface RgwRole {
  RoleId: string;
  RoleName: string;
  Path: string;
  Arn: string;
  CreateDate: string;
  MaxSessionDuration: number;
  AssumeRolePolicyDocument: string;
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
