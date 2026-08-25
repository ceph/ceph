import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from '@angular/core';
import { Observable, Subscriber, of } from 'rxjs';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { RgwIamPolicyService } from '~/app/shared/api/rgw-iam-policy.service';
import { IamPolicy } from '../models/rgw-iam-policy';
import { RgwAccountPolicyFormComponent } from '../rgw-account-policy-form/rgw-account-policy-form.component';
import { RgwAccountPolicyDetailModalComponent } from '../rgw-account-policy-detail-modal/rgw-account-policy-detail-modal.component';

@Component({
  selector: 'cd-rgw-account-policies-list',
  templateUrl: './rgw-account-policies-list.component.html',
  styleUrls: ['./rgw-account-policies-list.component.scss'],
  standalone: false
})
export class RgwAccountPoliciesListComponent implements OnInit, OnChanges {
  @Input()
  accountId: string;

  @Input()
  accountName: string;

  @ViewChild('table')
  table: TableComponent;

  columns: CdTableColumn[] = [];
  data$: Observable<IamPolicy[]>;
  tableActions: CdTableAction[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rgwIamPolicyService: RgwIamPolicyService,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private cdDatePipe: CdDatePipe,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit(): void {
    this.loadPolicies();
    this.columns = [
      {
        name: $localize`Policy Name`,
        prop: 'PolicyName',
        flexGrow: 2
      },
      {
        name: $localize`Arn`,
        prop: 'Arn',
        flexGrow: 3
      },
      {
        name: $localize`Default Version`,
        prop: 'DefaultVersionId',
        flexGrow: 1
      },
      {
        name: $localize`Created at`,
        prop: 'CreateDate',
        flexGrow: 2,
        pipe: this.cdDatePipe
      }
    ];

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.openPolicyForm(),
        name: this.actionLabels.CREATE,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        permission: 'read',
        icon: Icons.search,
        click: () => this.viewPolicyDetails(),
        name: $localize`View policy`,
        disable: () => !this.selection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deletePolicy(),
        name: this.actionLabels.DELETE,
        disable: () => !this.selection.hasSelection
      }
    ];
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.accountId) {
      this.loadPolicies();
    }
  }

  loadPolicies(): void {
    if (!this.accountId) {
      this.data$ = of([]);
      return;
    }
    this.data$ = this.rgwIamPolicyService.list(this.accountId);
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  openPolicyForm(): void {
    const modalRef = this.modalService.show(RgwAccountPolicyFormComponent, {
      accountId: this.accountId,
      accountName: this.accountName
    });
    modalRef?.close?.subscribe(() => this.loadPolicies());
  }

  viewPolicyDetails(): void {
    const policy = this.selection.first() as IamPolicy;
    if (!policy) {
      return;
    }

    const modalRef = this.modalService.show(RgwAccountPolicyDetailModalComponent, {
      policy,
      accountId: this.accountId
    });
    modalRef?.close?.subscribe(() => this.loadPolicies());
  }

  deletePolicy(): void {
    const policy = this.selection.first() as IamPolicy;
    if (!policy) {
      return;
    }

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`IAM Policy`,
      itemNames: [policy.PolicyName],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwIamPolicyService.delete(this.accountId, policy.PolicyName).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Policy deleted successfully`
              );
              observer.next();
              observer.complete();
              this.loadPolicies();
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
