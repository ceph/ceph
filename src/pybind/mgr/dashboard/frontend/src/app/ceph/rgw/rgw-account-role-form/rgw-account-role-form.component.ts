import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-rgw-account-role-form',
  templateUrl: './rgw-account-role-form.component.html',
  styleUrls: ['./rgw-account-role-form.component.scss'],
  standalone: false
})
export class RgwAccountRoleFormComponent extends BaseModal implements OnInit {
  form: CdFormGroup;

  constructor(
    @Optional() @Inject('accountId') public accountId: string,
    @Optional() @Inject('roleName') public roleName: string,
    @Optional() @Inject('isEdit') public isEdit = false,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit(): void {
    this.createForm();
    if (this.isEdit && this.roleName) {
      this.rgwRoleService.get(this.roleName, this.accountId).subscribe((role: any) => {
        this.form.patchValue({
          role_name: role.role_name,
          role_path: role.role_path || '/',
          max_session_duration: role.max_session_duration || 1
        });
      });
    }
  }

  private createForm() {
    this.form = this.formBuilder.group({
      role_name: [{ value: '', disabled: this.isEdit }, [Validators.required]],
      role_path: [{ value: '', disabled: this.isEdit }, [Validators.required]],
      role_assume_policy_doc: ['', this.isEdit ? [] : [Validators.required, CdValidators.json()]],
      max_session_duration: [
        1,
        this.isEdit ? [Validators.required, Validators.min(1), Validators.max(12)] : []
      ]
    });
  }

  onSubmit() {
    if (this.form.invalid) {
      return;
    }

    const payload = this.form.getRawValue();
    payload.account_id = this.accountId;
    if (!this.isEdit) {
      delete payload.max_session_duration;
    }

    if (this.isEdit) {
      this.rgwRoleService
        .update(this.roleName, {
          role_name: this.roleName,
          max_session_duration: payload.max_session_duration,
          account_id: this.accountId
        })
        .subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Role updated successfully`
            );
            this.closeModal();
          },
          error: () => {
            this.form.setErrors({ cdSubmitButton: true });
          }
        });
    } else {
      this.rgwRoleService.create(payload).subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Role created successfully`
          );
          this.closeModal();
        },
        error: () => {
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
    }
  }

  getMode() {
    return this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;
  }
}
