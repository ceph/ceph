import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { CephShortVersionPipe } from '~/app/shared/pipes/ceph-short-version.pipe';
import { JoinPipe } from '~/app/shared/pipes/join.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

const BASE_URL = 'hosts';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class HostsComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('servicesTpl', { static: true })
  public servicesTpl: TemplateRef<any>;

  permissions: Permissions;
  columns: Array<CdTableColumn> = [];
  hosts: Array<object> = [];
  isLoadingHosts = false;
  cdParams = { fromLink: '/hosts' };
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  modalRef: NgbModalRef;

  messages = {
    nonOrchHost: $localize`The feature is disabled because the selected host is not managed by Orchestrator.`
  };

  orchStatus: OrchestratorStatus;
  actionOrchFeatures = {
    create: [OrchestratorFeature.HOST_CREATE],
    edit: [OrchestratorFeature.HOST_LABEL_ADD, OrchestratorFeature.HOST_LABEL_REMOVE],
    delete: [OrchestratorFeature.HOST_DELETE]
  };

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private joinPipe: JoinPipe,
    private urlBuilder: URLBuilderService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    private notificationService: NotificationService,
    private orchService: OrchestratorService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigate([this.urlBuilder.getCreate()]),
        disable: (selection: CdTableSelection) => this.getDisable('create', selection)
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction(),
        disable: (selection: CdTableSelection) => this.getDisable('edit', selection)
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        disable: (selection: CdTableSelection) => this.getDisable('delete', selection)
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: $localize`Services`,
        prop: 'services',
        flexGrow: 3,
        cellTemplate: this.servicesTpl
      },
      {
        name: $localize`Labels`,
        prop: 'labels',
        flexGrow: 1,
        pipe: this.joinPipe
      },
      {
        name: $localize`Version`,
        prop: 'ceph_version',
        flexGrow: 1,
        pipe: this.cephShortVersionPipe
      }
    ];
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  editAction() {
    this.hostService.getLabels().subscribe((resp: string[]) => {
      const host = this.selection.first();
      const allLabels = resp.map((label) => {
        return { enabled: true, name: label };
      });
      this.modalService.show(FormModalComponent, {
        titleText: $localize`Edit Host: ${host.hostname}`,
        fields: [
          {
            type: 'select-badges',
            name: 'labels',
            value: host['labels'],
            label: $localize`Labels`,
            typeConfig: {
              customBadges: true,
              options: allLabels,
              messages: new SelectMessages({
                empty: $localize`There are no labels.`,
                filter: $localize`Filter or add labels`,
                add: $localize`Add label`
              })
            }
          }
        ],
        submitButtonText: $localize`Edit Host`,
        onSubmit: (values: any) => {
          this.hostService.update(host['hostname'], values.labels).subscribe(() => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Updated Host "${host.hostname}"`
            );
            // Reload the data table content.
            this.table.refreshBtn();
          });
        }
      });
    });
  }

  getDisable(action: 'create' | 'edit' | 'delete', selection: CdTableSelection): boolean | string {
    if (action === 'delete' || action === 'edit') {
      if (!selection?.hasSingleSelection) {
        return true;
      }
      if (!_.every(selection.selected, 'sources.orchestrator')) {
        return this.messages.nonOrchHost;
      }
    }
    return this.orchService.getTableActionDisableDesc(
      this.orchStatus,
      this.actionOrchFeatures[action]
    );
  }

  deleteAction() {
    const hostname = this.selection.first().hostname;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Host',
      itemNames: [hostname],
      actionDescription: 'delete',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('host/delete', { hostname: hostname }),
          call: this.hostService.delete(hostname)
        })
    });
  }

  getHosts(context: CdTableFetchDataContext) {
    if (this.isLoadingHosts) {
      return;
    }
    const typeToPermissionKey = {
      mds: 'cephfs',
      mon: 'monitor',
      osd: 'osd',
      rgw: 'rgw',
      'rbd-mirror': 'rbdMirroring',
      mgr: 'manager',
      'tcmu-runner': 'iscsi'
    };
    this.isLoadingHosts = true;
    this.hostService.list().subscribe(
      (resp: any[]) => {
        resp.map((host) => {
          host.services.map((service: any) => {
            service.cdLink = `/perf_counters/${service.type}/${encodeURIComponent(service.id)}`;
            const permission = this.permissions[typeToPermissionKey[service.type]];
            service.canRead = permission ? permission.read : false;
            return service;
          });
          return host;
        });
        this.hosts = resp;
        this.isLoadingHosts = false;
      },
      () => {
        this.isLoadingHosts = false;
        context.error();
      }
    );
  }
}
