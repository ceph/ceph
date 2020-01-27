import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { SettingsService } from '../api/settings.service';

@Injectable({
  providedIn: 'root'
})
export class PasswordPolicyService {
  constructor(private i18n: I18n, private settingsService: SettingsService) {}

  getHelpText(): Observable<string> {
    return this.settingsService
      .getValues([
        'PWD_POLICY_ENABLED',
        'PWD_POLICY_MIN_LENGTH',
        'PWD_POLICY_CHECK_LENGTH_ENABLED',
        'PWD_POLICY_CHECK_OLDPWD_ENABLED',
        'PWD_POLICY_CHECK_USERNAME_ENABLED',
        'PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED',
        'PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED',
        'PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED',
        'PWD_POLICY_CHECK_COMPLEXITY_ENABLED'
      ])
      .pipe(
        map((resp: Object[]) => {
          let helpText: string[] = [];
          if (resp['PWD_POLICY_ENABLED']) {
            helpText.push(this.i18n('Required rules for passwords:'));
            const i18nHelp: { [key: string]: string } = {
              PWD_POLICY_CHECK_LENGTH_ENABLED: this.i18n(
                'Must contain at least {{length}} characters',
                {
                  length: resp['PWD_POLICY_MIN_LENGTH']
                }
              ),
              PWD_POLICY_CHECK_OLDPWD_ENABLED: this.i18n(
                'Must not be the same as the previous one'
              ),
              PWD_POLICY_CHECK_USERNAME_ENABLED: this.i18n('Cannot contain the username'),
              PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: this.i18n(
                'Cannot contain any configured keyword'
              ),
              PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED: this.i18n(
                'Cannot contain any repetitive characters e.g. "aaa"'
              ),
              PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED: this.i18n(
                'Cannot contain any sequential characters e.g. "abc"'
              ),
              PWD_POLICY_CHECK_COMPLEXITY_ENABLED: this.i18n(
                'Must consist of characters from the following groups:\n' +
                  '  * Alphabetic a-z, A-Z\n' +
                  '  * Numbers 0-9\n' +
                  '  * Special chars: !"#$%& \'()*+,-./:;<=>?@[\\]^_`{{|}}~\n' +
                  '  * Any other characters (signs)'
              )
            };
            helpText = helpText.concat(
              Object.keys(i18nHelp)
                .filter((key) => resp[key])
                .map((key) => '- ' + i18nHelp[key])
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
