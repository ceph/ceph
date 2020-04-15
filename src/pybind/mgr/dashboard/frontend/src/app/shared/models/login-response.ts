export class LoginResponse {
  username: string;
  token: string;
  permissions: object;
  pwdExpirationDate: number;
  sso: boolean;
  pwdUpdateRequired: boolean;
}
