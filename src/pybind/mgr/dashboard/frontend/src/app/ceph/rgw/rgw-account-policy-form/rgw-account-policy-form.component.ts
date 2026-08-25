import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwIamPolicyService } from '~/app/shared/api/rgw-iam-policy.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DEFAULT_IAM_POLICY_DOCUMENT } from '../models/rgw-iam-policy';

@Component({
  selector: 'cd-rgw-account-policy-form',
  templateUrl: './rgw-account-policy-form.component.html',
  styleUrls: ['./rgw-account-policy-form.component.scss'],
  standalone: false
})
export class RgwAccountPolicyFormComponent extends BaseModal implements OnInit {
  form: CdFormGroup;

  constructor(
    @Optional() @Inject('accountId') public accountId: string,
    @Optional() @Inject('accountName') public accountName: string,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n,
    private rgwIamPolicyService: RgwIamPolicyService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit(): void {
    this.form = this.formBuilder.group({
      policy_name: ['', [Validators.required]],
      path: ['/', [Validators.required]],
      description: [''],
      policy_doc: [DEFAULT_IAM_POLICY_DOCUMENT, [Validators.required, CdValidators.json()]]
    });
  }

  onSubmit(): void {
    if (this.form.invalid) {
      return;
    }

    const payload = this.form.getRawValue();
    this.rgwIamPolicyService
      .create(this.accountId, {
        policy_name: payload.policy_name,
        policy_doc: payload.policy_doc,
        path: payload.path,
        description: payload.description || undefined
      })
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Policy created successfully`
          );
          this.closeModal();
        },
        error: () => {
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
  }
}
