import { Component, ChangeDetectorRef, Inject, OnInit, Optional } from '@angular/core';
import { AbstractControl, FormArray, FormGroup, Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { forkJoin } from 'rxjs';
import { finalize } from 'rxjs/operators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwRole } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-role-form',
  templateUrl: './rgw-account-role-form.component.html',
  styleUrls: ['./rgw-account-role-form.component.scss'],
  standalone: false
})
export class RgwAccountRoleFormComponent extends BaseModal implements OnInit {
  form: CdFormGroup;
  mode: string;
  isSubmitLoading = false;
  formSubmitted = false;
  icons = Icons;

  readonly steps = [{ label: $localize`Role details`, invalid: false }];
  title: string;
  modalHeaderLabel: string;
  description: string;

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
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef
  ) {
    super();
  }

  ngOnInit(): void {
    this.mode = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.title = this.isEdit ? $localize`Edit session duration` : $localize`Create role`;
    this.modalHeaderLabel = this.isEdit ? $localize`Role` : $localize`User account`;
    this.description = this.isEdit
      ? $localize`Update the maximum session duration for this role.`
      : $localize`Role grants temporary permissions to trusted users, applications, or services.`;

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
    this.permissionPolicies.push(
      this.formBuilder.group({
        policy_name: [name, [Validators.required]],
        policy_doc: [doc, [Validators.required, CdValidators.json()]]
      })
    );
  }

  removePermissionPolicy(index: number): void {
    this.permissionPolicies.removeAt(index);
  }

  /** Drop policy rows with no name and no document so optional policies stay optional. */
  private removeEmptyPermissionPolicies(): void {
    for (let i = this.permissionPolicies.length - 1; i >= 0; i--) {
      const group = this.permissionPolicies.at(i);
      const name = (group.get('policy_name')?.value ?? '').trim();
      const doc = (group.get('policy_doc')?.value ?? '').trim();
      if (!name && !doc) {
        this.permissionPolicies.removeAt(i);
      }
    }
  }

  private createForm() {
    this.form = this.formBuilder.group({
      role_name: [{ value: '', disabled: this.isEdit }, [Validators.required]],
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

  isFieldInvalid(controlName: string): boolean {
    const control = this.form.get(controlName);
    return !!control && control.invalid && (control.dirty || this.formSubmitted);
  }

  showFieldError(controlName: string, errorName?: string): boolean {
    const control = this.form.get(controlName);
    if (!control || !(control.dirty || this.formSubmitted)) {
      return false;
    }
    return errorName ? control.hasError(errorName) : control.invalid;
  }

  isPolicyFieldInvalid(policy: AbstractControl, fieldName: string): boolean {
    const control = policy.get(fieldName);
    // Optional policies: only show errors after the user interacts with a row,
    // or after submit when showValidationErrors() marks that row dirty.
    return !!control && control.invalid && control.dirty;
  }

  showPolicyFieldError(policy: AbstractControl, fieldName: string, errorName?: string): boolean {
    const control = policy.get(fieldName);
    if (!control || !control.dirty) {
      return false;
    }
    return errorName ? control.hasError(errorName) : control.invalid;
  }

  /** Surface field errors when Create is clicked on an invalid tearsheet form. */
  private showValidationErrors(): void {
    this.formSubmitted = true;
    this.form.markAllAsTouched();
    Object.values(this.form.controls).forEach((control) => {
      if (control instanceof FormArray) {
        control.controls.forEach((group) => {
          if (group instanceof FormGroup) {
            Object.values(group.controls).forEach((child) => {
              child.markAsTouched();
              child.markAsDirty();
            });
          }
        });
      } else {
        control.markAsDirty();
      }
    });
    this.cdr.markForCheck();
  }

  onSubmit() {
    if (!this.isEdit) {
      this.removeEmptyPermissionPolicies();
    }

    if (this.form.invalid) {
      this.showValidationErrors();
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
      this.rgwRoleService.create(payload).subscribe({
        next: () => {
          if (permissionPolicies.length > 0) {
            const policyCalls = permissionPolicies.map((p) =>
              this.rgwRoleService.putPolicy(
                payload.role_name,
                p.policy_name,
                p.policy_doc,
                this.accountId
              )
            );
            forkJoin(policyCalls)
              .pipe(
                finalize(() => {
                  this.isSubmitLoading = false;
                })
              )
              .subscribe({
                next: () => {
                  this.notificationService.show(
                    NotificationType.success,
                    $localize`Role created with permission policies`,
                    $localize`Role "${payload.role_name}" created and policies attached successfully.`
                  );
                  this.closeModal();
                },
                error: () => {
                  this.notificationService.show(
                    NotificationType.warning,
                    $localize`Role created`,
                    $localize`Role "${payload.role_name}" created, but failed to attach permission policies.`
                  );
                  this.closeModal();
                }
              });
          } else {
            this.isSubmitLoading = false;
            this.notificationService.show(
              NotificationType.success,
              $localize`Role created successfully`
            );
            this.closeModal();
          }
        },
        error: () => {
          this.isSubmitLoading = false;
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
    }
  }
}
