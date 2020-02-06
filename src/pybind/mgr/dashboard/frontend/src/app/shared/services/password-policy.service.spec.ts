import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { SettingsService } from '../api/settings.service';
import { SharedModule } from '../shared.module';
import { PasswordPolicyService } from './password-policy.service';

describe('PasswordPolicyService', () => {
  let service: PasswordPolicyService;
  let settingsService: SettingsService;

  const helpTextHelper = {
    get: (chk: string) => {
      const chkTexts: { [key: string]: string } = {
        chk_length: 'Must contain at least 10 characters',
        chk_oldpwd: 'Must not be the same as the previous one',
        chk_username: 'Cannot contain the username',
        chk_exclusion_list: 'Cannot contain any configured keyword',
        chk_repetitive: 'Cannot contain any repetitive characters e.g. "aaa"',
        chk_sequential: 'Cannot contain any sequential characters e.g. "abc"',
        chk_complexity:
          'Must consist of characters from the following groups:\n' +
          '  * Alphabetic a-z, A-Z\n' +
          '  * Numbers 0-9\n' +
          '  * Special chars: !"#$%& \'()*+,-./:;<=>?@[\\]^_`{{|}}~\n' +
          '  * Any other characters (signs)'
      };
      return ['Required rules for passwords:', '- ' + chkTexts[chk]].join('\n');
    }
  };

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    service = TestBed.get(PasswordPolicyService);
    settingsService = TestBed.get(SettingsService);
    settingsService['settings'] = {};
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should not get help text', () => {
    let helpText = '';
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe('');
  });

  it('should get help text chk_length', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_length');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_MIN_LENGTH: 10,
        PWD_POLICY_CHECK_LENGTH_ENABLED: true,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: false,
        PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED: false,
        PWD_POLICY_CHECK_COMPLEXITY_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_oldpwd', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_oldpwd');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: true,
        PWD_POLICY_CHECK_USERNAME_ENABLED: false,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: false,
        PWD_POLICY_CHECK_COMPLEXITY_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_username', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_username');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: false,
        PWD_POLICY_CHECK_USERNAME_ENABLED: true,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_exclusion_list', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_exclusion_list');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_CHECK_USERNAME_ENABLED: false,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: true,
        PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_repetitive', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_repetitive');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: false,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: false,
        PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED: true,
        PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED: false,
        PWD_POLICY_CHECK_COMPLEXITY_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_sequential', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_sequential');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_MIN_LENGTH: 8,
        PWD_POLICY_CHECK_LENGTH_ENABLED: false,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: false,
        PWD_POLICY_CHECK_USERNAME_ENABLED: false,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: false,
        PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED: false,
        PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED: true,
        PWD_POLICY_CHECK_COMPLEXITY_ENABLED: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_complexity', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_complexity');
    spyOn(settingsService, 'getValues').and.returnValue(
      observableOf({
        PWD_POLICY_ENABLED: true,
        PWD_POLICY_MIN_LENGTH: 8,
        PWD_POLICY_CHECK_LENGTH_ENABLED: false,
        PWD_POLICY_CHECK_OLDPWD_ENABLED: false,
        PWD_POLICY_CHECK_USERNAME_ENABLED: false,
        PWD_POLICY_CHECK_EXCLUSION_LIST_ENABLED: false,
        PWD_POLICY_CHECK_REPETITIVE_CHARS_ENABLED: false,
        PWD_POLICY_CHECK_SEQUENTIAL_CHARS_ENABLED: false,
        PWD_POLICY_CHECK_COMPLEXITY_ENABLED: true
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get too-weak class', () => {
    expect(service.mapCreditsToCssClass(0)).toBe('too-weak');
    expect(service.mapCreditsToCssClass(9)).toBe('too-weak');
  });

  it('should get weak class', () => {
    expect(service.mapCreditsToCssClass(10)).toBe('weak');
    expect(service.mapCreditsToCssClass(14)).toBe('weak');
  });

  it('should get ok class', () => {
    expect(service.mapCreditsToCssClass(15)).toBe('ok');
    expect(service.mapCreditsToCssClass(19)).toBe('ok');
  });

  it('should get strong class', () => {
    expect(service.mapCreditsToCssClass(20)).toBe('strong');
    expect(service.mapCreditsToCssClass(24)).toBe('strong');
  });

  it('should get very-strong class', () => {
    expect(service.mapCreditsToCssClass(25)).toBe('very-strong');
    expect(service.mapCreditsToCssClass(30)).toBe('very-strong');
  });
});
