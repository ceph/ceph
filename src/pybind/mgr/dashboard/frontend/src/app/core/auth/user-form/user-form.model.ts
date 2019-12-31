export class UserFormModel {
  username: string;
  password: string;
  pwdExpirationDate: number;
  name: string;
  email: string;
  roles: Array<string>;
  enabled: boolean;
}
