import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
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
    imports: [HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    service = TestBed.inject(PasswordPolicyService);
    settingsService = TestBed.inject(SettingsService);
    settingsService['settings'] = {};
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should not get help text', () => {
    let helpText = '';
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe('');
  });

  it('should get help text chk_length', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_length');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        user_pwd_expiration_warning_1: 10,
        user_pwd_expiration_warning_2: 5,
        user_pwd_expiration_span: 90,
        pwd_policy_enabled: true,
        pwd_policy_min_length: 10,
        pwd_policy_check_length_enabled: true,
        pwd_policy_check_oldpwd_enabled: false,
        pwd_policy_check_sequential_chars_enabled: false,
        pwd_policy_check_complexity_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_oldpwd', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_oldpwd');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: true,
        pwd_policy_check_oldpwd_enabled: true,
        pwd_policy_check_username_enabled: false,
        pwd_policy_check_exclusion_list_enabled: false,
        pwd_policy_check_complexity_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_username', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_username');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: true,
        pwd_policy_check_oldpwd_enabled: false,
        pwd_policy_check_username_enabled: true,
        pwd_policy_check_exclusion_list_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_exclusion_list', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_exclusion_list');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: true,
        pwd_policy_check_username_enabled: false,
        pwd_policy_check_exclusion_list_enabled: true,
        pwd_policy_check_repetitive_chars_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_repetitive', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_repetitive');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        user_pwd_expiration_warning_1: 10,
        pwd_policy_enabled: true,
        pwd_policy_check_oldpwd_enabled: false,
        pwd_policy_check_exclusion_list_enabled: false,
        pwd_policy_check_repetitive_chars_enabled: true,
        pwd_policy_check_sequential_chars_enabled: false,
        pwd_policy_check_complexity_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_sequential', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_sequential');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: true,
        pwd_policy_min_length: 8,
        pwd_policy_check_length_enabled: false,
        pwd_policy_check_oldpwd_enabled: false,
        pwd_policy_check_username_enabled: false,
        pwd_policy_check_exclusion_list_enabled: false,
        pwd_policy_check_repetitive_chars_enabled: false,
        pwd_policy_check_sequential_chars_enabled: true,
        pwd_policy_check_complexity_enabled: false
      })
    );
    service.getHelpText().subscribe((text) => (helpText = text));
    expect(helpText).toBe(expectedHelpText);
  });

  it('should get help text chk_complexity', () => {
    let helpText = '';
    const expectedHelpText = helpTextHelper.get('chk_complexity');
    spyOn(settingsService, 'getStandardSettings').and.returnValue(
      observableOf({
        pwd_policy_enabled: true,
        pwd_policy_min_length: 8,
        pwd_policy_check_length_enabled: false,
        pwd_policy_check_oldpwd_enabled: false,
        pwd_policy_check_username_enabled: false,
        pwd_policy_check_exclusion_list_enabled: false,
        pwd_policy_check_repetitive_chars_enabled: false,
        pwd_policy_check_sequential_chars_enabled: false,
        pwd_policy_check_complexity_enabled: true
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
