import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { SettingsService } from '../api/settings.service';
import { CdPwdPolicySettings } from '../models/cd-pwd-policy-settings';

@Injectable({
  providedIn: 'root'
})
export class PasswordPolicyService {
  constructor(private settingsService: SettingsService) {}

  getHelpText(): Observable<string> {
    return this.settingsService.getStandardSettings().pipe(
      map((resp: { [key: string]: any }) => {
        const settings = new CdPwdPolicySettings(resp);
        let helpText: string[] = [];
        if (settings.pwdPolicyEnabled) {
          helpText.push($localize`Required rules for passwords:`);
          const i18nHelp: { [key: string]: string } = {
            pwdPolicyCheckLengthEnabled: $localize`Must contain at least ${settings.pwdPolicyMinLength} characters`,
            pwdPolicyCheckOldpwdEnabled: $localize`Must not be the same as the previous one`,
            pwdPolicyCheckUsernameEnabled: $localize`Cannot contain the username`,
            pwdPolicyCheckExclusionListEnabled: $localize`Cannot contain any configured keyword`,
            pwdPolicyCheckRepetitiveCharsEnabled: $localize`Cannot contain any repetitive characters e.g. "aaa"`,
            pwdPolicyCheckSequentialCharsEnabled: $localize`Cannot contain any sequential characters e.g. "abc"`,
            pwdPolicyCheckComplexityEnabled: $localize`Must consist of characters from the following groups:
  * Alphabetic a-z, A-Z
  * Numbers 0-9
  * Special chars: !"#$%& '()*+,-./:;<=>?@[\\]^_\`{{|}}~
  * Any other characters (signs)`
          };
          helpText = helpText.concat(
            _.keys(i18nHelp)
              .filter((key) => _.get(settings, key))
              .map((key) => '- ' + _.get(i18nHelp, key))
          );
        }
        return helpText.join('\n');
      })
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
