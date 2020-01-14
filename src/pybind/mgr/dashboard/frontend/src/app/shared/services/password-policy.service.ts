import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

@Injectable({
  providedIn: 'root'
})
export class PasswordPolicyService {
  constructor(private i18n: I18n) {}

  getHelpText() {
    return this.i18n(
      'Required rules for password complexity:\n\
    - must contain at least 8 characters\n\
    - cannot contain username\n\
    - cannot contain any keyword used in Ceph\n\
    - cannot contain any repetitive characters e.g. "aaa"\n\
    - cannot contain any sequential characters e.g. "abc"\n\
    - must consist of characters from the following groups:\n\
      * alphabetic a-z, A-Z\n\
      * numbers 0-9\n\
      * special chars: !"#$%& \'()*+,-./:;<=>?@[\\]^_`{{|}}~\n\
      * any other characters (signs)'
    );
  }

  /**
   * Helper function to map password policy credits to a CSS class.
   * @param credits The password policy credits.
   * @return The name of the CSS class.
   */
  mapCreditsToCssClass(credits: number): string {
    let result = 'very-strong';
    if (credits < 10) {
      result = 'too-weak';
    } else if (credits < 15) {
      result = 'weak';
    } else if (credits < 20) {
      result = 'ok';
    } else if (credits < 25) {
      result = 'strong';
    }
    return result;
  }
}
