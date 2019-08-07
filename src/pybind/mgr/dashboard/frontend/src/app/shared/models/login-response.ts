export class LoginResponse {
  username: string;
  token: string;
  permissions: object;
  sso: boolean;
  force_change_pwd: boolean;
}
