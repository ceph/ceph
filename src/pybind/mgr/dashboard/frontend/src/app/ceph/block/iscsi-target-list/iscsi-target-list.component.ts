import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { Subscription } from 'rxjs';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { Task } from '../../../shared/models/task';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { NotAvailablePipe } from '../../../shared/pipes/not-available.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { IscsiTargetDiscoveryModalComponent } from '../iscsi-target-discovery-modal/iscsi-target-discovery-modal.component';

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
  docsUrl: string;
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
    private i18n: I18n,
    private iscsiService: IscsiService,
    private taskListService: TaskListService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private notAvailablePipe: NotAvailablePipe,
    private summaryservice: SummaryService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
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
        disable: () => !this.selection.first() || !_.isUndefined(this.getDeleteDisableDesc()),
        disableDesc: () => this.getEditDisableDesc()
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteIscsiTargetModal(),
        name: this.actionLabels.DELETE,
        disable: () => !this.selection.first() || !_.isUndefined(this.getDeleteDisableDesc()),
        disableDesc: () => this.getDeleteDisableDesc()
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Target'),
        prop: 'target_iqn',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: this.i18n('Portals'),
        prop: 'cdPortals',
        flexGrow: 2
      },
      {
        name: this.i18n('Images'),
        prop: 'cdImages',
        flexGrow: 2
      },
      {
        name: this.i18n('# Sessions'),
        prop: 'info.num_sessions',
        pipe: this.notAvailablePipe,
        flexGrow: 1
      }
    ];

    this.iscsiService.status().subscribe((result: any) => {
      this.available = result.available;

      if (result.available) {
        this.iscsiService.version().subscribe((res: any) => {
          this.cephIscsiConfigVersion = res['ceph_iscsi_config_version'];
          this.taskListService.init(
            () => this.iscsiService.listTargets(),
            (resp) => this.prepareResponse(resp),
            (targets) => (this.targets = targets),
            () => this.onFetchError(),
            this.taskFilter,
            this.itemFilter,
            this.builders
          );
        });

        this.iscsiService.settings().subscribe((settings: any) => {
          this.settings = settings;
        });
      } else {
        this.summaryservice.subscribeOnce((summary) => {
          const releaseName = this.cephReleaseNamePipe.transform(summary.version);
          this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/dashboard/#enabling-iscsi-management`;
          this.status = result.message;
        });
      }
    });
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  getEditDisableDesc(): string | undefined {
    const first = this.selection.first();
    if (first && first.cdExecuting) {
      return first.cdExecuting;
    }
    if (first && _.isUndefined(first['info'])) {
      return this.i18n('Unavailable gateway(s)');
    }

    return undefined;
  }

  getDeleteDisableDesc(): string | undefined {
    const first = this.selection.first();
    if (first && first.cdExecuting) {
      return first.cdExecuting;
    }
    if (first && _.isUndefined(first['info'])) {
      return this.i18n('Unavailable gateway(s)');
    }
    if (first && first['info'] && first['info']['num_sessions']) {
      return this.i18n('Target has active sessions');
    }

    return undefined;
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

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: this.i18n('iSCSI target'),
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
