import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges,
  ViewEncapsulation
} from '@angular/core';
import { Validators } from '@angular/forms';
import { Observable, Subscriber } from 'rxjs';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-rgw-account-role-policies-side-panel',
  templateUrl: './rgw-account-role-policies-side-panel.component.html',
  styleUrls: ['./rgw-account-role-policies-side-panel.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwAccountRolePoliciesSidePanelComponent implements OnInit, OnChanges {
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
    this.checkToLoadPolicy();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.expanded || changes.policyName || changes.roleName || changes.accountId) {
      if (!this.expanded) {
        this.resetState();
        return;
      }
      this.checkToLoadPolicy();
    }
  }

  private checkToLoadPolicy(): void {
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
    this.policyDocumentLoading = true;
    this.rgwRoleService.getPolicy(this.roleName, policyName, this.accountId).subscribe({
      next: (policyDocument: string) => {
        this.selectedPolicyDocument = policyDocument;
        this.policyDocumentLoading = false;
      },
      error: () => {
        this.policyDocumentLoading = false;
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

    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.medium,
      itemDescription: $localize`policy`,
      itemNames: [policyName],
      actionDescription: $localize`remove`,
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
              this.changed.emit();
              this.close();
            },
            error: (err) => observer.error(err)
          });
        });
      }
    });
  }
}
