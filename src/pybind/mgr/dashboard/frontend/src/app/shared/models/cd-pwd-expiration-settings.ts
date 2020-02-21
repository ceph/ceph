export class CdPwdExpirationSettings {
  pwdExpirationSpan = 0;
  pwdExpirationWarning1: number;
  pwdExpirationWarning2: number;

  constructor(settings: { [key: string]: any }) {
    this.pwdExpirationSpan = settings.user_pwd_expiration_span;
    this.pwdExpirationWarning1 = settings.user_pwd_expiration_warning_1;
    this.pwdExpirationWarning2 = settings.user_pwd_expiration_warning_2;
  }
}
