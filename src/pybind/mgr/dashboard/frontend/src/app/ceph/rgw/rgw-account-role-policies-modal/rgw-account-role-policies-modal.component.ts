import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges
} from '@angular/core';
import { Validators } from '@angular/forms';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-rgw-account-role-policies-modal',
  templateUrl: './rgw-account-role-policies-modal.component.html',
  styleUrls: ['./rgw-account-role-policies-modal.component.scss'],
  standalone: false
})
export class RgwAccountRolePoliciesModalComponent implements OnInit, OnChanges {
  @Input() expanded = false;
  @Input() accountId = '';
  @Input() roleName = '';
  @Input() policyName = '';

  @Output() closed = new EventEmitter<void>();
  @Output() changed = new EventEmitter<void>();

  icons = Icons;
  selectedPolicyName = '';
  selectedPolicyDocument = '';
  policyDocumentLoading = false;
  /** Deferred so CodeSnippet canExpand() does not trip NG0100. */
  showPolicySnippet = false;
  isEditing = false;
  isSubmitLoading = false;
  form: CdFormGroup;

  constructor(
    private formBuilder: CdFormBuilder,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.form = this.formBuilder.group({
      policy_name: [{ value: '', disabled: true }, [Validators.required]],
      policy_doc: ['', [Validators.required, CdValidators.json()]]
    });
  }

  ngOnInit(): void {
    this.maybeLoadPolicy();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.expanded || changes.policyName || changes.roleName || changes.accountId) {
      if (!this.expanded) {
        this.resetState();
        return;
      }
      this.maybeLoadPolicy();
    }
  }

  private maybeLoadPolicy(): void {
    if (this.expanded && this.roleName && this.accountId && this.policyName) {
      this.loadPolicy(this.policyName);
    }
  }

  get panelHeaderText(): string {
    if (!this.selectedPolicyName) {
      return '';
    }
    return this.isEditing ? $localize`Edit ${this.selectedPolicyName}` : this.selectedPolicyName;
  }

  get panelHeaderDescription(): string {
    if (!this.roleName) {
      return '';
    }
    return $localize`Permission policy - ${this.roleName}`;
  }

  close(): void {
    this.closed.emit();
  }

  private resetState(): void {
    this.selectedPolicyName = '';
    this.selectedPolicyDocument = '';
    this.showPolicySnippet = false;
    this.policyDocumentLoading = false;
    this.isEditing = false;
    this.isSubmitLoading = false;
  }

  private loadPolicy(policyName: string): void {
    if (!policyName || !this.roleName || !this.accountId) {
      return;
    }
    this.isEditing = false;
    this.selectedPolicyName = policyName;
    this.selectedPolicyDocument = '';
    this.showPolicySnippet = false;
    this.policyDocumentLoading = true;
    this.rgwRoleService.getPolicy(this.roleName, policyName, this.accountId).subscribe({
      next: (res: any) => {
        this.selectedPolicyDocument = this.normalizePolicyDocument(res);
        this.policyDocumentLoading = false;
        // Defer snippet mount so AfterViewInit height check does not cause NG0100.
        setTimeout(() => {
          this.showPolicySnippet = true;
        });
      },
      error: () => {
        this.policyDocumentLoading = false;
        this.showPolicySnippet = false;
      }
    });
  }

  startEdit(): void {
    if (!this.selectedPolicyName) {
      return;
    }
    this.form.patchValue({
      policy_name: this.selectedPolicyName,
      policy_doc: this.selectedPolicyDocument
    });
    this.isEditing = true;
  }

  cancelEdit(): void {
    this.isEditing = false;
    setTimeout(() => {
      this.showPolicySnippet = true;
    });
  }

  saveChanges(): void {
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
            $localize`Permission updated`,
            $localize`User role '${this.roleName}' permission '${policy_name}' updated.`
          );
          this.isEditing = false;
          this.changed.emit();
          this.loadPolicy(policy_name);
        },
        error: () => {
          this.isSubmitLoading = false;
          this.form.setErrors({ cdSubmitButton: true });
        }
      });
  }

  deleteSelectedPolicy(): void {
    const policyName = this.selectedPolicyName;
    const roleName = this.roleName;
    if (!policyName || !roleName) {
      return;
    }

    const modalRef = this.modalService.show(ConfirmationModalComponent, {
      headerLabel: $localize`Remove policy`,
      titleText: $localize`Confirm remove`,
      description: $localize`Removing ${policyName} will immediately revoke the permissions it grants to this role. As a result, any users, applications, or services relying on this role will lose access to the associated storage resources and will no longer be able to perform actions on them.`,
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
            modalRef.closeModal();
            this.changed.emit();
            this.close();
          },
          error: () => {
            modalRef.stopLoadingSpinner();
          }
        });
      }
    });
  }

  private normalizePolicyDocument(res: any): string {
    let policyDoc = res;

    if (typeof res === 'object' && res !== null) {
      const keys = Object.keys(res);
      const policyKey = keys.find(
        (k) =>
          /policy/i.test(k) || k === 'Permission policy' || k === 'Policy' || k === 'PolicyDocument'
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
        return policyDoc;
      }
    }

    if (typeof policyDoc === 'object' && policyDoc !== null) {
      return JSON.stringify(policyDoc, null, 2);
    }

    return String(policyDoc ?? '');
  }
}
