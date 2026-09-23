import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-rgw-account-role-policy-form',
  templateUrl: './rgw-account-role-policy-form.component.html',
  styleUrls: ['./rgw-account-role-policy-form.component.scss'],
  standalone: false
})
export class RgwAccountRolePolicyFormComponent extends BaseModal implements OnInit {
  form: CdFormGroup;
  action: string;
  icons = Icons;
  isSubmitLoading = false;

  readonly steps = [{ label: $localize`Policy details`, invalid: false }];
  title: string;
  modalHeaderLabel: string;
  description: string;

  constructor(
    @Optional() @Inject('accountId') public accountId: string,
    @Optional() @Inject('roleName') public roleName: string,
    @Optional() @Inject('policyName') public policyName: string,
    @Optional() @Inject('isEdit') public isEdit = false,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit(): void {
    this.action = this.isEdit ? this.actionLabels.EDIT : $localize`Attach`;
    this.modalHeaderLabel = this.roleName || $localize`Role`;
    this.title = this.isEdit
      ? $localize`Edit ‘${this.policyName}’ permission`
      : $localize`Attach permission policy`;
    this.description = this.isEdit
      ? $localize`Update or delete this permission policy.`
      : $localize`Attach an IAM-compatible JSON policy to this role.`;
    this.createForm();
    if (this.isEdit && this.policyName) {
      this.loadPolicy();
    }
  }

  private createForm() {
    this.form = this.formBuilder.group({
      policy_name: [{ value: this.policyName || '', disabled: this.isEdit }, [Validators.required]],
      policy_doc: ['', [Validators.required, CdValidators.json()]]
    });
  }

  private loadPolicy() {
    this.rgwRoleService
      .getPolicy(this.roleName, this.policyName, this.accountId)
      .subscribe((res: any) => {
        let policyDoc = res;

        if (typeof res === 'object' && res !== null) {
          const keys = Object.keys(res);
          const policyKey = keys.find(
            (k) =>
              /policy/i.test(k) ||
              k === 'Permission policy' ||
              k === 'Policy' ||
              k === 'PolicyDocument'
          );
          if (policyKey && res[policyKey]) {
            policyDoc = res[policyKey];
          } else if (keys.length === 1) {
            policyDoc = res[keys[0]];
          }
        }

        if (typeof policyDoc === 'string') {
          try {
            policyDoc = JSON.parse(policyDoc);
          } catch {
            // Keep as string if not valid JSON
          }
        }

        if (typeof policyDoc === 'object' && policyDoc !== null) {
          policyDoc = JSON.stringify(policyDoc, null, 2);
        }

        this.form.patchValue({ policy_doc: policyDoc });
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
      .putPolicy(this.roleName, policy_name, policy_doc, this.accountId)
      .subscribe({
        next: () => {
          this.isSubmitLoading = false;
          this.notificationService.show(
            NotificationType.success,
            this.isEdit
              ? $localize`Permission policy updated`
              : $localize`Permission policy attached`,
            this.isEdit
              ? $localize`Policy "${policy_name}" updated for role "${this.roleName}" successfully.`
              : $localize`Policy "${policy_name}" attached to role "${this.roleName}" successfully.`
          );
          this.closeModal();
        },
        error: () => {
          this.isSubmitLoading = false;
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
  }

  deletePolicy(): void {
    const policyName = this.policyName || this.form.get('policy_name')?.value;
    const roleName = this.roleName;

    if (!policyName || !roleName) {
      return;
    }

    const modalRef = this.modalService.show(ConfirmationModalComponent, {
      modalHeaderLabel: $localize`Remove ${policyName}`,
      titleText: $localize`Confirm remove`,
      description: $localize`Removing ${policyName} will permanently remove this permission policy.`,
      buttonText: $localize`Remove`,
      submitBtnType: 'danger',
      onSubmit: () => {
        this.rgwRoleService.deletePolicy(roleName, policyName, this.accountId).subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Policy detached successfully`,
              $localize`Policy "${policyName}" detached from role "${roleName}".`
            );
            this.modalService.dismissAll();
            this.closeModal();
          },
          error: () => {
            modalRef.stopLoadingSpinner();
          }
        });
      }
    });
  }
}
