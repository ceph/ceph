import { Component, NgZone, OnDestroy, OnInit, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';

import { IscsiService } from '~/app/shared/api/iscsi.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { Task } from '~/app/shared/models/task';
import { JoinPipe } from '~/app/shared/pipes/join.pipe';
import { NotAvailablePipe } from '~/app/shared/pipes/not-available.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { IscsiTargetDiscoveryModalComponent } from '../iscsi-target-discovery-modal/iscsi-target-discovery-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

@Component({
  selector: 'cd-iscsi-target-list',
  templateUrl: './iscsi-target-list.component.html',
  styleUrls: ['./iscsi-target-list.component.scss'],
  providers: [TaskListService]
})
export class IscsiTargetListComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild(TableComponent)
  table: TableComponent;

  available: boolean = undefined;
  columns: CdTableColumn[];
  modalRef: NgbModalRef;
  permission: Permission;
  selection = new CdTableSelection();
  cephIscsiConfigVersion: number;
  settings: any;
  status: string;
  summaryDataSubscription: Subscription;
  tableActions: CdTableAction[];
  targets: any[] = [];
  icons = Icons;

  builders = {
    'iscsi/target/create': (metadata: object) => {
      return {
        target_iqn: metadata['target_iqn']
      };
    }
  };

  constructor(
    private authStorageService: AuthStorageService,
    private iscsiService: IscsiService,
    private joinPipe: JoinPipe,
    private taskListService: TaskListService,
    private notAvailablePipe: NotAvailablePipe,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n,
    protected ngZone: NgZone
  ) {
    super(ngZone);
    this.permission = this.authStorageService.getPermissions().iscsi;

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        routerLink: () => '/block/iscsi/targets/create',
        name: this.actionLabels.CREATE
      },
      {
        permission: 'update',
        icon: Icons.edit,
        routerLink: () => `/block/iscsi/targets/edit/${this.selection.first().target_iqn}`,
        name: this.actionLabels.EDIT,
        disable: () => this.getEditDisableDesc()
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteIscsiTargetModal(),
        name: this.actionLabels.DELETE,
        disable: () => this.getDeleteDisableDesc()
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Target`,
        prop: 'target_iqn',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: $localize`Portals`,
        prop: 'cdPortals',
        pipe: this.joinPipe,
        flexGrow: 2
      },
      {
        name: $localize`Images`,
        prop: 'cdImages',
        pipe: this.joinPipe,
        flexGrow: 2
      },
      {
        name: $localize`# Sessions`,
        prop: 'info.num_sessions',
        pipe: this.notAvailablePipe,
        flexGrow: 1
      }
    ];

    this.iscsiService.status().subscribe((result: any) => {
      this.available = result.available;

      if (!result.available) {
        this.status = result.message;
      }
    });
  }

  getTargets() {
    if (this.available) {
      this.setTableRefreshTimeout();
      this.iscsiService.version().subscribe((res: any) => {
        this.cephIscsiConfigVersion = res['ceph_iscsi_config_version'];
      });
      this.taskListService.init(
        () => this.iscsiService.listTargets(),
        (resp) => this.prepareResponse(resp),
        (targets) => (this.targets = targets),
        () => this.onFetchError(),
        this.taskFilter,
        this.itemFilter,
        this.builders
      );

      this.iscsiService.settings().subscribe((settings: any) => {
        this.settings = settings;
      });
    }
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  getEditDisableDesc(): string | boolean {
    const first = this.selection.first();

    if (first && first?.cdExecuting) {
      return first.cdExecuting;
    }

    if (first && _.isUndefined(first?.['info'])) {
      return $localize`Unavailable gateway(s)`;
    }

    return !first;
  }

  getDeleteDisableDesc(): string | boolean {
    const first = this.selection.first();

    if (first?.cdExecuting) {
      return first.cdExecuting;
    }

    if (first && _.isUndefined(first?.['info'])) {
      return $localize`Unavailable gateway(s)`;
    }

    if (first && first?.['info']?.['num_sessions']) {
      return $localize`Target has active sessions`;
    }

    return !first;
  }

  prepareResponse(resp: any): any[] {
    resp.forEach((element: Record<string, any>) => {
      element.cdPortals = element.portals.map(
        (portal: Record<string, any>) => `${portal.host}:${portal.ip}`
      );
      element.cdImages = element.disks.map(
        (disk: Record<string, any>) => `${disk.pool}/${disk.image}`
      );
    });

    return resp;
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
  }

  itemFilter(entry: Record<string, any>, task: Task) {
    return entry.target_iqn === task.metadata['target_iqn'];
  }

  taskFilter(task: Task) {
    return ['iscsi/target/create', 'iscsi/target/edit', 'iscsi/target/delete'].includes(task.name);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteIscsiTargetModal() {
    const target_iqn = this.selection.first().target_iqn;

    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`iSCSI target`,
      itemNames: [target_iqn],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('iscsi/target/delete', {
            target_iqn: target_iqn
          }),
          call: this.iscsiService.deleteTarget(target_iqn)
        })
    });
  }

  configureDiscoveryAuth() {
    this.modalService.show(IscsiTargetDiscoveryModalComponent);
  }
}
