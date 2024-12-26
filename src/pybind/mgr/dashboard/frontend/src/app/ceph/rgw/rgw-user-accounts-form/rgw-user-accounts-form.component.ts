import { Component } from '@angular/core';
import { AbstractControl, ValidationErrors, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { Account } from '../models/rgw-user-accounts';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdValidators, isEmptyInputValue } from '~/app/shared/forms/cd-validators';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { Observable, concat as observableConcat } from 'rxjs';

@Component({
  selector: 'cd-rgw-user-accounts-form',
  templateUrl: './rgw-user-accounts-form.component.html',
  styleUrls: ['./rgw-user-accounts-form.component.scss']
})
export class RgwUserAccountsFormComponent extends CdForm {
  accountForm: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean = false;
  submitObservables: Observable<Object>[] = [];

  constructor(
    private router: Router,
    private actionLabels: ActionLabelsI18n,
    private rgwUserAccountsService: RgwUserAccountsService,
    private notificationService: NotificationService,
    private formBuilder: CdFormBuilder
  ) {
    super();
    this.editing = this.router.url.includes('rgw/accounts/edit');
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`Account`;
    this.createForm();
    this.loadingReady();
  }

  private createForm() {
    this.accountForm = this.formBuilder.group({
      account_id: [''],
      tenant: [''],
      account_name: ['', Validators.required],
      email: ['', CdValidators.email],
      max_users_mode: [1],
      max_users: [
        1000,
        [CdValidators.requiredIf({ max_users_mode: '1' }), CdValidators.number(false)]
      ],
      max_roles_mode: [1],
      max_roles: [
        1000,
        [CdValidators.requiredIf({ max_roles_mode: '1' }), CdValidators.number(false)]
      ],
      max_group_mode: [1],
      max_group: [
        1000,
        [CdValidators.requiredIf({ max_group_mode: '1' }), CdValidators.number(false)]
      ],
      max_access_keys_mode: [1],
      max_access_keys: [
        4,
        [CdValidators.requiredIf({ max_access_keys_mode: '1' }), CdValidators.number(false)]
      ],
      max_buckets_mode: [1],
      max_buckets: [
        1000,
        [CdValidators.requiredIf({ max_buckets_mode: '1' }), CdValidators.number(false)]
      ],
      account_quota_enabled: [false],
      account_quota_max_size_unlimited: [true],
      account_quota_max_size: [
        null,
        [
          CdValidators.composeIf(
            {
              account_quota_enabled: true,
              account_quota_max_size_unlimited: false
            },
            [Validators.required, this.quotaMaxSizeValidator]
          )
        ]
      ],
      account_quota_max_objects_unlimited: [true],
      account_quota_max_objects: [
        null,
        [
          CdValidators.requiredIf({
            account_quota_enabled: true,
            account_quota_max_objects_unlimited: false
          }),
          Validators.pattern(/^[0-9]+$/)
        ]
      ],
      bucket_quota_enabled: [false],
      bucket_quota_max_size_unlimited: [true],
      bucket_quota_max_size: [
        null,
        [
          CdValidators.composeIf(
            {
              bucket_quota_enabled: true,
              bucket_quota_max_size_unlimited: false
            },
            [Validators.required, this.quotaMaxSizeValidator]
          )
        ]
      ],
      bucket_quota_max_objects_unlimited: [true],
      bucket_quota_max_objects: [
        null,
        [
          CdValidators.requiredIf({
            bucket_quota_enabled: true,
            bucket_quota_max_objects_unlimited: false
          }),
          Validators.pattern(/^[0-9]+$/)
        ]
      ]
    });
  }

  /**
   * Validate the quota maximum size, e.g. 1096, 1K, 30M or 1.9MiB.
   */
  quotaMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    if (isEmptyInputValue(control.value)) {
      return null;
    }
    const m = RegExp('^(\\d+(\\.\\d+)?)\\s*(B|K(B|iB)?|M(B|iB)?|G(B|iB)?|T(B|iB)?)?$', 'i').exec(
      control.value
    );
    if (m === null) {
      return { quotaMaxSize: true };
    }
    const bytes = new FormatterService().toBytes(control.value);
    return bytes < 1024 ? { quotaMaxSize: true } : null;
  }

  submit() {
    let notificationTitle: string = '';
    if (this.accountForm.invalid) {
      return;
    }

    if (this.accountForm.pending) {
      this.accountForm.setErrors({ cdSubmitButton: true });
      return;
    }

    if (!this.editing) {
      const formvalue = this.accountForm.value;
      const createPayload = {
        account_id: formvalue.account_id,
        account_name: formvalue.account_name,
        email: formvalue.email,
        tenant: formvalue.tenant,
        max_users: this.getValueFromFormControl('max_users'),
        max_buckets: this.getValueFromFormControl('max_buckets'),
        max_roles: this.getValueFromFormControl('max_roles'),
        max_group: this.getValueFromFormControl('max_group'),
        max_access_keys: this.getValueFromFormControl('max_access_keys')
      };
      notificationTitle = $localize`Account created successfully`;
      this.rgwUserAccountsService.create(createPayload).subscribe({
        next: (account: Account) => {
          this.accountForm.get('account_id').setValue(account.id);
          this.setQuotaConfig();
          this.notificationService.show(NotificationType.success, notificationTitle);
        },
        error: () => {
          // Reset the 'Submit' button.
          this.accountForm.setErrors({ cdSubmitButton: true });
        }
      });
    }
  }

  setQuotaConfig() {
    const accountId: string = this.accountForm.get('account_id').value;
    // Check if account quota has been modified.
    if (this._isQuotaConfDirty('account')) {
      const accountQuotaArgs = this._getQuotaArgs('account');
      this.submitObservables.push(
        this.rgwUserAccountsService.setQuota(accountId, accountQuotaArgs)
      );
    }
    // Check if bucket quota has been modified.
    if (this._isQuotaConfDirty('bucket')) {
      const bucketQuotaArgs = this._getQuotaArgs('bucket');
      this.submitObservables.push(this.rgwUserAccountsService.setQuota(accountId, bucketQuotaArgs));
    }
    // Finally execute all observables one by one in serial.
    observableConcat(...this.submitObservables).subscribe({
      error: () => {
        // Reset the 'Submit' button.
        this.accountForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => {
        this.goToListView();
      }
    });
    if (this.submitObservables.length == 0) {
      this.goToListView();
    }
  }

  /**
   * Helper function to get the arguments for the API request when any
   * quota configuration has been modified.
   */
  private _getQuotaArgs(quotaType: string) {
    const result = {
      quota_type: quotaType,
      enabled: this.accountForm.getValue(`${quotaType}_quota_enabled`),
      max_size: '-1',
      max_objects: '-1'
    };
    if (!this.accountForm.getValue(`${quotaType}_quota_max_size_unlimited`)) {
      // Convert the given value to bytes.
      const bytes = new FormatterService().toBytes(
        this.accountForm.getValue(`${quotaType}_quota_max_size`)
      );
      // Finally convert the value to KiB.
      result['max_size'] = (bytes / 1024).toFixed(0) as any;
    }
    if (!this.accountForm.getValue(`${quotaType}_quota_max_objects_unlimited`)) {
      result['max_objects'] = `${this.accountForm.getValue(`${quotaType}_quota_max_objects`)}`;
    }
    return result;
  }

  /**
   * Check if any quota has been modified.
   * @return {Boolean} Returns TRUE if the quota has been modified.
   */
  private _isQuotaConfDirty(quotaType: string): boolean {
    if (this.accountForm.get(`${quotaType}_quota_enabled`).value) {
      return [
        `${quotaType}_quota_enabled`,
        `${quotaType}_quota_max_size_unlimited`,
        `${quotaType}_quota_max_size`,
        `${quotaType}_quota_max_objects_unlimited`,
        `${quotaType}_quota_max_objects`
      ].some((path) => {
        return this.accountForm.get(path).dirty;
      });
    }
    return false;
  }

  onModeChange(mode: string, formControlName: string) {
    if (mode === '1') {
      // If 'Custom' mode is selected, then ensure that the form field
      // 'Max. buckets' contains a valid value. Set it to default if
      // necessary.
      if (!this.accountForm.get(formControlName).valid) {
        this.accountForm.patchValue({
          [formControlName]: 1000
        });
      }
    }
  }

  goToListView(): void {
    this.router.navigate(['rgw/accounts']);
  }

  getValueFromFormControl(formControlName: string) {
    const formvalue = this.accountForm.value;
    return formvalue[`${formControlName}_mode`] == 1
      ? formvalue[formControlName]
      : formvalue[`${formControlName}_mode`];
  }
}
