import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class UserChangePasswordService {
  requiredPasswordRulesMessage: string;
  passwordStrengthLevel: string;
  passwordStrengthDescription: string;

  constructor() {}
  getPasswordRulesMessage() {
    return `Required  rules for password complexity:
        - cannot contain username
        - cannot contain any keyword used in Ceph
        - must  consist of characters from the following groups:
            * alphabetic a-z, A-Z
            * numbers 0-9
            * special chars: "!"#$%&\'()*+,-./:;<=>?@[\\]^_\`{|}~"
            * any other characters (signs)`;
  }

  checkPasswordComplexity(password) {
    this.passwordStrengthLevel = 'passwordStrengthLevel0';
    this.passwordStrengthDescription = '';
    const credits = this.checkPasswordComplexityLetters(password);
    if (credits) {
      if (credits < 10) {
        this.passwordStrengthLevel = 'passwordStrengthLevel0';
        this.passwordStrengthDescription = 'Too weak';
      } else {
        if (credits < 15) {
          this.passwordStrengthLevel = 'passwordStrengthLevel1';
          this.passwordStrengthDescription = 'Weak';
        } else {
          if (credits < 20) {
            this.passwordStrengthLevel = 'passwordStrengthLevel2';
            this.passwordStrengthDescription = 'OK';
          } else {
            if (credits < 25) {
              this.passwordStrengthLevel = 'passwordStrengthLevel3';
              this.passwordStrengthDescription = 'Strong';
            } else {
              this.passwordStrengthLevel = 'passwordStrengthLevel4';
              this.passwordStrengthDescription = 'Very strong';
            }
          }
        }
      }
    }
    return [this.passwordStrengthLevel, this.passwordStrengthDescription];
  }

  private checkPasswordComplexityLetters(password): number {
    const digitsNumber = password.replace(/[^0-9]/g, '').length;
    const smallLettersNumber = password.replace(/[^a-z]/g, '').length;
    const bigLettersNumber = password.replace(/[^A-Z]/g, '').length;
    const punctuationNumber = password.replace(/[^!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~]/g, '').length;
    const othersCharactersNumber =
      password.length - (digitsNumber + smallLettersNumber + bigLettersNumber + punctuationNumber);
    return (
      digitsNumber +
      smallLettersNumber +
      bigLettersNumber * 2 +
      punctuationNumber * 3 +
      othersCharactersNumber * 5
    );
  }
}
