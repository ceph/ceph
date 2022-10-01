export class LoginResponse {
  username: string;
  permissions: object;
  pwdExpirationDate: number;
  sso: boolean;
  pwdUpdateRequired: boolean;
}
