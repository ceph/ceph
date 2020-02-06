export class CdPwdExpirationSettings {
  pwdExpirationSpan = 0;
  pwdExpirationWarning1: number;
  pwdExpirationWarning2: number;

  constructor(
    pwdExpirationSpan: number,
    pwdExpirationWarning1: number,
    pwdExpirationWarning2: number
  ) {
    this.pwdExpirationSpan = pwdExpirationSpan;
    this.pwdExpirationWarning1 = pwdExpirationWarning1;
    this.pwdExpirationWarning2 = pwdExpirationWarning2;
  }
}
