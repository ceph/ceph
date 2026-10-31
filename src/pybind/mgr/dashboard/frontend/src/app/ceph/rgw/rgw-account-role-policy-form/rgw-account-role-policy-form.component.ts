import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-rgw-account-role-policy-form',
  templateUrl: './rgw-account-role-policy-form.component.html',
  standalone: false
})
export class RgwAccountRolePolicyFormComponent extends BaseModal implements OnInit {
  form: CdFormGroup;
  isSubmitLoading = false;

  readonly steps = [{ label: $localize`Policy details`, invalid: false }];
  title: string;
  modalHeaderLabel: string;
  description: string;

  constructor(
    @Optional() @Inject('accountId') public accountId: string,
    @Optional() @Inject('roleName') public roleName: string,
    private formBuilder: CdFormBuilder,
    private rgwRoleService: RgwRoleService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit(): void {
    this.modalHeaderLabel = this.roleName || $localize`Role`;
    this.title = $localize`Attach permission policy`;
    this.description = $localize`Attach an IAM-compatible JSON policy to this role.`;
    this.createForm();
  }

  private createForm() {
    this.form = this.formBuilder.group({
      policy_name: ['', [Validators.required]],
      policy_doc: ['', [Validators.required, CdValidators.json()]]
    });
  }

  onSubmit() {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    const { policy_name, policy_doc } = this.form.getRawValue();
    this.isSubmitLoading = true;

    this.rgwRoleService
      .attachPolicy(this.roleName, policy_name, policy_doc, this.accountId)
      .subscribe({
        next: () => {
          this.isSubmitLoading = false;
          this.notificationService.show(
            NotificationType.success,
            $localize`Permission policy attached`,
            $localize`Policy "${policy_name}" attached to role "${this.roleName}" successfully.`
          );
          this.closeModal();
        },
        error: () => {
          this.isSubmitLoading = false;
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
  }
}
