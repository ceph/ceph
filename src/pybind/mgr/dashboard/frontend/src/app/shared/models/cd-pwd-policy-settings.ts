export class CdPwdPolicySettings {
  pwdPolicyEnabled: boolean;
  pwdPolicyMinLength: number;
  pwdPolicyCheckLengthEnabled: boolean;
  pwdPolicyCheckOldpwdEnabled: boolean;
  pwdPolicyCheckUsernameEnabled: boolean;
  pwdPolicyCheckExclusionListEnabled: boolean;
  pwdPolicyCheckRepetitiveCharsEnabled: boolean;
  pwdPolicyCheckSequentialCharsEnabled: boolean;
  pwdPolicyCheckComplexityEnabled: boolean;

  constructor(settings: { [key: string]: any }) {
    this.pwdPolicyEnabled = settings.pwd_policy_enabled;
    this.pwdPolicyMinLength = settings.pwd_policy_min_length;
    this.pwdPolicyCheckLengthEnabled = settings.pwd_policy_check_length_enabled;
    this.pwdPolicyCheckOldpwdEnabled = settings.pwd_policy_check_oldpwd_enabled;
    this.pwdPolicyCheckUsernameEnabled = settings.pwd_policy_check_username_enabled;
    this.pwdPolicyCheckExclusionListEnabled = settings.pwd_policy_check_exclusion_list_enabled;
    this.pwdPolicyCheckRepetitiveCharsEnabled = settings.pwd_policy_check_repetitive_chars_enabled;
    this.pwdPolicyCheckSequentialCharsEnabled = settings.pwd_policy_check_sequential_chars_enabled;
    this.pwdPolicyCheckComplexityEnabled = settings.pwd_policy_check_complexity_enabled;
  }
}
