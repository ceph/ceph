import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { RgwAccountRolePolicyFormComponent } from '../rgw-account-role-policy-form/rgw-account-role-policy-form.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Observable, Subscriber, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { RgwRole } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-role-details',
  templateUrl: './rgw-account-role-details.component.html',
  styleUrls: ['./rgw-account-role-details.component.scss'],
  standalone: false
})
export class RgwAccountRoleDetailsComponent implements OnInit, OnChanges {
  @Input()
  selection: RgwRole;

  @Input()
  accountId: string;

  columns: CdTableColumn[] = [];
  policies$: Observable<any[]>;
  tableActions: CdTableAction[] = [];
  policySelection: CdTableSelection = new CdTableSelection();
  permission: Permission;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Policy name`,
        prop: 'name',
        flexGrow: 1
      }
    ];

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.openAttachPolicyModal(),
        name: $localize`Attach policy`,
        canBePrimary: () => true
      },
      {
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openEditPolicyModal(),
        name: this.actionLabels.EDIT,
        disable: () => !this.policySelection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deletePolicy(),
        name: this.actionLabels.DELETE,
        disable: () => !this.policySelection.hasSelection
      }
    ];

    this.loadPolicies();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.selection && this.selection) {
      this.loadPolicies();
    }
  }

  get roleName(): string {
    if (!this.selection) {
      return '';
    }
    return (
      this.selection.RoleName ||
      (this.selection as any).role_name ||
      (this.selection as any).row?.RoleName ||
      (this.selection as any).row?.role_name ||
      ''
    );
  }

  get roleArn(): string {
    return this.selection?.Arn || '';
  }

  get rolePath(): string {
    return this.selection?.Path || '/';
  }

  get maxSessionDuration(): string {
    return this.selection?.MaxSessionDuration
      ? `${this.selection.MaxSessionDuration / 3600} hours`
      : '1 hour';
  }

  get overviewFields(): OverviewField[] {
    return [
      {
        label: $localize`Role name`,
        value: this.roleName
      },
      {
        label: $localize`Role ARN`,
        value: this.roleArn || '-'
      },
      {
        label: $localize`Path`,
        value: this.rolePath
      },
      {
        label: $localize`Max session duration`,
        value: this.maxSessionDuration
      }
    ];
  }

  get trustPolicyJson(): string {
    const doc = (this.selection as any)?.AssumeRolePolicyDocument;
    if (!doc) {
      return '';
    }
    if (typeof doc === 'object') {
      return JSON.stringify(doc, null, 2);
    }
    try {
      return JSON.stringify(JSON.parse(doc), null, 2);
    } catch {
      return String(doc);
    }
  }

  loadPolicies(): void {
    const roleName = this.roleName;
    if (!roleName || !this.accountId) {
      this.policies$ = of([]);
      return;
    }

    this.policies$ = this.rgwRoleService.listPolicies(roleName, this.accountId).pipe(
      map((policies: string[]) => {
        return (policies || []).map((name) => ({ name }));
      })
    );
  }

  updateSelection(selection: CdTableSelection): void {
    this.policySelection = selection;
  }

  openAttachPolicyModal(): void {
    const modalRef = this.modalService.show(RgwAccountRolePolicyFormComponent, {
      accountId: this.accountId,
      roleName: this.roleName
    });
    modalRef?.close?.subscribe(() => this.loadPolicies());
  }

  icons = Icons;

  openEditPolicyModal(policyName?: string): void {
    const name =
      policyName || (this.policySelection.hasSelection ? this.policySelection.first().name : '');
    if (!name) {
      return;
    }
    const modalRef = this.modalService.show(RgwAccountRolePolicyFormComponent, {
      accountId: this.accountId,
      roleName: this.roleName,
      policyName: name,
      isEdit: true
    });
    modalRef?.close?.subscribe(() => this.loadPolicies());
  }

  deletePolicy(policyName?: string): void {
    const name =
      policyName || (this.policySelection.hasSelection ? this.policySelection.first().name : '');
    if (!name) {
      return;
    }
    const roleName = this.roleName;

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Permission policy`,
      itemNames: [name],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.deletePolicy(roleName, name, this.accountId).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Policy detached successfully`,
                $localize`Policy "${name}" detached from role "${roleName}".`
              );
              this.loadPolicies();
              observer.next();
              observer.complete();
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
