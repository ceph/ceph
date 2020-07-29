import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { HostService } from '../../../shared/api/host.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '../../../shared/components/form-modal/form-modal.component';
import { SelectMessages } from '../../../shared/components/select/select-messages.model';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permissions } from '../../../shared/models/permissions';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { JoinPipe } from '../../../shared/pipes/join.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { DepCheckerService } from '../../../shared/services/dep-checker.service';
import { ModalService } from '../../../shared/services/modal.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

const BASE_URL = 'hosts';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class HostsComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent, { static: true })
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
    private depCheckerService: DepCheckerService,
    private notificationService: NotificationService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => {
          this.depCheckerService.checkOrchestratorOrModal(
            this.actionLabels.CREATE,
            $localize`Host`,
            () => {
              this.router.navigate([this.urlBuilder.getCreate()]);
            }
          );
        }
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => {
          this.depCheckerService.checkOrchestratorOrModal(
            this.actionLabels.EDIT,
            $localize`Host`,
            () => this.editAction()
          );
        },
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection || !selection.first().sources.orchestrator,
        disableDesc: this.getEditDisableDesc.bind(this)
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => {
          this.depCheckerService.checkOrchestratorOrModal(
            this.actionLabels.DELETE,
            $localize`Host`,
            () => this.deleteAction()
          );
        },
        disable: (selection: CdTableSelection) =>
          !selection.hasSelection ||
          !selection.selected.every((selected) => selected.sources.orchestrator),
        disableDesc: this.getDeleteDisableDesc.bind(this)
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

  getEditDisableDesc(selection: CdTableSelection): string | undefined {
    if (selection && selection.hasSingleSelection && !selection.first().sources.orchestrator) {
      return $localize`Host editing is disabled because the selected host is not managed by Orchestrator.`;
    }
    return undefined;
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

  getDeleteDisableDesc(selection: CdTableSelection): string | undefined {
    if (
      selection &&
      selection.hasSelection &&
      !selection.selected.every((selected) => selected.sources.orchestrator)
    ) {
      return $localize`Host deletion is disabled because a selected host is not managed by Orchestrator.`;
    }
    return undefined;
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
