export class CdPwdExpirationSettings {
  pwdExpirationSpan = 0;
  pwdExpirationWarning1: number;
  pwdExpirationWarning2: number;

  constructor(data) {
    this.pwdExpirationSpan = data.user_pwd_expiration_span;
    this.pwdExpirationWarning1 = data.user_pwd_expiration_warning_1;
    this.pwdExpirationWarning2 = data.user_pwd_expiration_warning_2;
  }
}
