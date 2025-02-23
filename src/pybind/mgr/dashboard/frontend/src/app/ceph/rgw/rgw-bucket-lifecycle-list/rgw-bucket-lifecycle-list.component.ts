import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { Permission } from '~/app/shared/models/permissions';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwBucketTieringFormComponent } from '../rgw-bucket-tiering-form/rgw-bucket-tiering-form.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { Observable, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-rgw-bucket-lifecycle-list',
  templateUrl: './rgw-bucket-lifecycle-list.component.html',
  styleUrls: ['./rgw-bucket-lifecycle-list.component.scss']
})
export class RgwBucketLifecycleListComponent implements OnInit {
  @Input() bucket: Bucket;
  @Output() updateBucketDetails = new EventEmitter();
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  filteredLifecycleRules$: Observable<any[]>;
  lifecycleRuleList: any = [];
  modalRef: any;

  constructor(
    private rgwBucketService: RgwBucketService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private notificationService: NotificationService
  ) {}

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'ID',
        flexGrow: 2
      },
      {
        name: $localize`Days`,
        prop: 'Transition.Days',
        flexGrow: 1
      },
      {
        name: $localize`Storage class`,
        prop: 'Transition.StorageClass',
        flexGrow: 1
      },
      {
        name: $localize`Status`,
        prop: 'Status',
        flexGrow: 1
      }
    ];
    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      click: () => this.openTieringModal(this.actionLabels.CREATE),
      name: this.actionLabels.CREATE
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      disable: () => this.selection.hasMultiSelection,
      click: () => this.openTieringModal(this.actionLabels.EDIT),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteAction(),
      disable: () => !this.selection.hasSelection,
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.tableActions = [createAction, editAction, deleteAction];
  }

  loadLifecyclePolicies(context: CdTableFetchDataContext) {
    const allLifecycleRules$ = this.rgwBucketService
      .getLifecycle(this.bucket.bucket, this.bucket.owner)
      .pipe(
        tap((lifecycle) => {
          this.lifecycleRuleList = lifecycle;
        }),
        catchError(() => {
          context.error();
          return of(null);
        })
      );

    this.filteredLifecycleRules$ = allLifecycleRules$.pipe(
      map(
        (lifecycle: any) =>
          lifecycle?.LifecycleConfiguration?.Rules?.filter((rule: object) =>
            rule.hasOwnProperty('Transition')
          ) || []
      )
    );
  }

  openTieringModal(type: string) {
    const modalRef = this.modalService.show(RgwBucketTieringFormComponent, {
      bucket: this.bucket,
      selectedLifecycle: this.selection.first(),
      editing: type === this.actionLabels.EDIT ? true : false
    });
    modalRef?.close?.subscribe(() => this.updateBucketDetails.emit());
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const ruleNames = this.selection.selected.map((rule) => rule.ID);
    const filteredRules = this.lifecycleRuleList.LifecycleConfiguration.Rules.filter(
      (rule: any) => !ruleNames.includes(rule.ID)
    );
    const rules = filteredRules.length > 0 ? { Rules: filteredRules } : {};
    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Rule`,
      itemNames: ruleNames,
      actionDescription: $localize`remove`,
      submitAction: () => this.submitLifecycleConfig(rules)
    });
  }

  submitLifecycleConfig(rules: any) {
    this.rgwBucketService
      .setLifecycle(this.bucket.bucket, JSON.stringify(rules), this.bucket.owner)
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Lifecycle rule deleted successfully`
          );
          this.updateBucketDetails.emit();
        },
        error: () => {
          this.modalRef.componentInstance.stopLoadingSpinner();
        },
        complete: () => {
          this.modalService.dismissAll();
        }
      });
  }
}
