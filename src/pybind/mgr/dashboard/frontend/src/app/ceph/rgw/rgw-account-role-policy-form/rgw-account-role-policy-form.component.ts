import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { Observable, Subscriber } from 'rxjs';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { PREDEFINED_POLICY_TEMPLATES } from '../utils/constants';

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

  policyTypeControl = new FormControl<'predefined' | 'custom'>('predefined');

  predefinedTemplates = PREDEFINED_POLICY_TEMPLATES;
  selectedTemplateControl = new FormControl<string>(PREDEFINED_POLICY_TEMPLATES[0].name);
  attachedPolicies: string[] = [];

  get allPredefinedAttached(): boolean {
    return this.predefinedTemplates.every((t) => this.isTemplateAttached(t.name));
  }

  isTemplateAttached(templateName: string): boolean {
    return this.attachedPolicies.includes(templateName);
  }

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
    if (this.isEdit) {
      this.policyTypeControl.setValue('custom');
    }
    this.createForm();
    if (this.isEdit && this.policyName) {
      this.loadPolicy();
    } else if (!this.isEdit && this.roleName && this.accountId) {
      this.loadAttachedPolicies();
    }
  }

  private loadAttachedPolicies(): void {
    this.rgwRoleService.listPolicies(this.roleName, this.accountId).subscribe({
      next: (policies: string[]) => {
        this.attachedPolicies = policies || [];
        // If the currently selected template is already attached, pick the first available unattached one
        if (this.isTemplateAttached(this.selectedTemplateControl.value)) {
          const available = this.predefinedTemplates.find((t) => !this.isTemplateAttached(t.name));
          if (available) {
            this.selectedTemplateControl.setValue(available.name);
            this.applyTemplate(available.name);
          } else {
            // All templates attached, leave on Select a policy template... and clear form
            this.selectedTemplateControl.setValue('');
            this.applyTemplate('');
          }
        }
      },
      error: () => {
        this.attachedPolicies = [];
      }
    });
  }

  onPolicyTypeChange(event: any): void {
    const selectedType = event?.value || event;
    this.policyTypeControl.setValue(selectedType);

    if (selectedType === 'predefined') {
      this.applyTemplate(this.selectedTemplateControl.value);
    } else if (!this.isEdit) {
      this.form.patchValue({
        policy_name: '',
        policy_doc: ''
      });
    }
  }

  onTemplateChange(event: any): void {
    const templateName = event?.target?.value || event?.value || event;
    this.selectedTemplateControl.setValue(templateName);
    this.applyTemplate(templateName);
  }

  private applyTemplate(templateName: string): void {
    if (!templateName) {
      this.form?.patchValue({
        policy_name: '',
        policy_doc: ''
      });
      return;
    }
    const tpl = this.predefinedTemplates.find((t) => t.name === templateName);
    if (tpl) {
      this.form.patchValue({
        policy_name: tpl.name,
        policy_doc: tpl.policy_doc
      });
    }
  }

  private createForm() {
    const isPredefined = !this.isEdit && this.policyTypeControl.value === 'predefined';
    const initialTpl = this.predefinedTemplates.find((t) => !this.isTemplateAttached(t.name));

    this.form = this.formBuilder.group({
      policy_name: [
        {
          value: isPredefined && initialTpl ? initialTpl.name : this.policyName || '',
          disabled: this.isEdit
        },
        [Validators.required]
      ],
      policy_doc: [
        isPredefined && initialTpl ? initialTpl.policy_doc : '',
        [Validators.required, CdValidators.json()]
      ]
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

    this.rgwRoleService
      .putPolicy(this.roleName, policy_name, policy_doc, this.accountId)
      .subscribe({
        next: () => {
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

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Permission policy`,
      itemNames: [policyName],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.deletePolicy(roleName, policyName, this.accountId).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Policy detached successfully`,
                $localize`Policy "${policyName}" detached from role "${roleName}".`
              );
              observer.next();
              observer.complete();
              this.closeModal();
            },
            error: (err) => {
              observer.error(err);
            }
          });
        });
      }
    });
  }
}
