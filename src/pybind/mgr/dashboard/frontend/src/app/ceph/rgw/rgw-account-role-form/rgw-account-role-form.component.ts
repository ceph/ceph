import { Component, Inject, OnInit, Optional, ViewChild } from '@angular/core';
import { FormArray, NgForm, Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { forkJoin, of } from 'rxjs';
import { catchError, finalize, map, switchMap } from 'rxjs/operators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwRole } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-role-form',
  templateUrl: './rgw-account-role-form.component.html',
  styleUrls: ['./rgw-account-role-form.component.scss'],
  standalone: false
})
export class RgwAccountRoleFormComponent extends BaseModal implements OnInit {
  @ViewChild('formDir') formDir: NgForm;

  form: CdFormGroup;
  mode: string;
  isSubmitLoading = false;

  readonly steps = [{ label: $localize`Role details`, invalid: false }];
  title: string;
  modalHeaderLabel: string;
  description: string;
  submitButtonLabel: string;
  submitButtonLoadingLabel: string;

  readonly policyDocPlaceholder = $localize`Provide a valid JSON permission policy.`;

  constructor(
    @Optional() @Inject('accountId') public accountId: string,
    @Optional() @Inject('accountName') public accountName: string,
    @Optional() @Inject('roleName') public roleName: string,
    @Optional() @Inject('isEdit') public isEdit = false,
    @Optional() @Inject('role') public role: RgwRole = null,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit(): void {
    this.mode = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.title = this.isEdit ? $localize`Edit session duration` : $localize`Create role`;
    this.modalHeaderLabel = this.isEdit ? $localize`Role` : $localize`User account`;
    this.description = this.isEdit
      ? $localize`Update the maximum session duration for this role.`
      : $localize`A role defines the permissions that users, applications, or services can use when accessing object storage resources.`;
    this.submitButtonLabel = this.isEdit
      ? this.actionLabels.SAVE_CHANGES
      : this.actionLabels.CREATE;
    this.submitButtonLoadingLabel = this.isEdit
      ? this.actionLabels.SAVING
      : this.actionLabels.CREATING;

    this.createForm();
    if (this.isEdit && this.role) {
      this.form.patchValue({
        role_name: this.role.RoleName,
        role_path: this.role.Path || '/',
        max_session_duration: this.role.MaxSessionDuration ? this.role.MaxSessionDuration / 3600 : 1
      });
    }
  }

  get permissionPolicies(): FormArray {
    return this.form.get('permission_policies') as FormArray;
  }

  addPermissionPolicy(name = '', doc = ''): void {
    const group = this.formBuilder.group({
      policy_name: [
        name,
        [
          Validators.required,
          CdValidators.custom('uniqueName', (value: string) => {
            const val = (value ?? '').trim().toLowerCase();
            if (!val || !this.permissionPolicies) {
              return false;
            }
            const count = this.permissionPolicies.controls.filter((ctrl) => {
              const otherVal = (ctrl.get('policy_name')?.value ?? '').trim().toLowerCase();
              return otherVal === val;
            }).length;
            return count > 1;
          })
        ]
      ],
      policy_doc: [doc, [Validators.required, CdValidators.json()]]
    });
    group.get('policy_name')?.valueChanges.subscribe(() => {
      this.revalidatePolicyNames();
    });
    this.permissionPolicies.push(group);
    this.revalidatePolicyNames();
  }

  removePermissionPolicy(index: number): void {
    this.permissionPolicies.removeAt(index);
    this.revalidatePolicyNames();
  }

  private revalidatePolicyNames(): void {
    this.permissionPolicies.controls.forEach((ctrl) => {
      ctrl.get('policy_name')?.updateValueAndValidity({ emitEvent: false });
    });
  }

  private createForm() {
    this.form = this.formBuilder.group({
      role_name: [
        { value: '', disabled: this.isEdit },
        [Validators.required],
        this.isEdit
          ? []
          : [
              CdValidators.unique(
                this.rgwRoleService.exists,
                this.rgwRoleService,
                null,
                false,
                this.accountId
              )
            ]
      ],
      role_path: [{ value: '', disabled: this.isEdit }, [Validators.required]],
      role_assume_policy_doc: [''],
      permission_policies: this.formBuilder.array([]),
      max_session_duration: [1]
    });

    CdValidators.validateIf(this.form.get('role_assume_policy_doc'), () => !this.isEdit, [
      Validators.required,
      CdValidators.json()
    ]);

    CdValidators.validateIf(this.form.get('max_session_duration'), () => this.isEdit, [
      Validators.required,
      Validators.min(1),
      Validators.max(12)
    ]);
  }

  onSubmit() {
    if (this.form.invalid) {
      // Tearsheet Create/Save is a footer button, not a native form submit.
      // Mark form submitted + touched so cdValidate / form.showError show field errors
      // (including permission-policy FormArray rows once they have been added).
      this.form.markAllAsTouched();
      this.formDir?.onSubmit(undefined);
      return;
    }

    const payload = this.form.getRawValue();
    payload.account_id = this.accountId;
    if (!this.isEdit) {
      delete payload.max_session_duration;
    }

    const permissionPolicies: { policy_name: string; policy_doc: string }[] =
      !this.isEdit && this.permissionPolicies
        ? this.permissionPolicies.controls.map((ctrl) => ctrl.getRawValue())
        : [];
    delete payload.permission_policies;

    this.isSubmitLoading = true;

    if (this.isEdit) {
      this.rgwRoleService
        .update(this.roleName, {
          role_name: this.roleName,
          max_session_duration: payload.max_session_duration,
          account_id: this.accountId
        })
        .pipe(
          finalize(() => {
            this.isSubmitLoading = false;
          })
        )
        .subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Session duration updated`,
              $localize`The maximum session duration for role "${this.roleName}" has been updated to ${payload.max_session_duration} hours.`
            );
            this.closeModal();
          },
          error: () => {
            this.form.setErrors({ cdSubmitButton: true });
          }
        });
    } else {
      this.rgwRoleService
        .create(payload)
        .pipe(
          switchMap(() => {
            if (permissionPolicies.length === 0) {
              return of([]);
            }
            // Isolate each attach so one failure does not cancel the rest.
            const policyCalls = permissionPolicies.map((policy) =>
              this.rgwRoleService
                .attachPolicy(
                  payload.role_name,
                  policy.policy_name,
                  policy.policy_doc,
                  this.accountId
                )
                .pipe(
                  map(() => ({ policy_name: policy.policy_name, success: true as const })),
                  catchError(() => of({ policy_name: policy.policy_name, success: false as const }))
                )
            );
            return forkJoin(policyCalls);
          }),
          finalize(() => {
            this.isSubmitLoading = false;
          })
        )
        .subscribe({
          next: (results) => {
            const failed = results
              .filter((result) => !result.success)
              .map((result) => result.policy_name);
            if (permissionPolicies.length > 0) {
              if (!failed.length) {
                this.notificationService.show(
                  NotificationType.success,
                  $localize`Role created with permission policies`,
                  $localize`Role "${payload.role_name}" created and policies attached successfully.`
                );
                this.closeModal();
                return;
              }

              this.notificationService.show(
                NotificationType.warning,
                $localize`Role created`,
                $localize`Role "${payload.role_name}" created, but failed to attach: ${failed.join(
                  ', '
                )}.`
              );
              // Keep tearsheet open so the user can see which policies failed.
              this.form.setErrors({ cdSubmitButton: true });
            } else {
              this.notificationService.show(
                NotificationType.success,
                $localize`Role created successfully`
              );
              this.closeModal();
            }
          },
          error: () => {
            this.form.setErrors({ cdSubmitButton: true });
          }
        });
    }
  }
}
