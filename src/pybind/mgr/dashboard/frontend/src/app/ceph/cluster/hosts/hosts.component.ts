import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

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
  modalRef: BsModalRef;

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private joinPipe: JoinPipe,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    private actionLabels: ActionLabelsI18n,
    private modalService: BsModalService,
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
            this.i18n('Host'),
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
            this.i18n('Host'),
            () => this.editAction()
          );
        },
        disable: (selection: CdTableSelection) =>
          !selection.hasSingleSelection || !selection.first().sources.orchestrator,
        disableDesc: () => this.getEditDisableDesc()
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => {
          this.depCheckerService.checkOrchestratorOrModal(
            this.actionLabels.DELETE,
            this.i18n('Host'),
            () => this.deleteAction()
          );
        },
        disable: () => !this.selection.hasSelection
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: this.i18n('Services'),
        prop: 'services',
        flexGrow: 3,
        cellTemplate: this.servicesTpl
      },
      {
        name: this.i18n('Labels'),
        prop: 'labels',
        flexGrow: 1,
        pipe: this.joinPipe
      },
      {
        name: this.i18n('Version'),
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
        initialState: {
          titleText: this.i18n('Edit Host: {{hostname}}', host),
          fields: [
            {
              type: 'select-badges',
              name: 'labels',
              value: host['labels'],
              label: this.i18n('Labels'),
              typeConfig: {
                customBadges: true,
                options: allLabels,
                messages: new SelectMessages(
                  {
                    empty: this.i18n('There are no labels.'),
                    filter: this.i18n('Filter or add labels'),
                    add: this.i18n('Add label')
                  },
                  this.i18n
                )
              }
            }
          ],
          submitButtonText: this.i18n('Edit Host'),
          onSubmit: (values: any) => {
            this.hostService.update(host['hostname'], values.labels).subscribe(() => {
              this.notificationService.show(
                NotificationType.success,
                this.i18n('Updated Host "{{hostname}}"', host)
              );
              // Reload the data table content.
              this.table.refreshBtn();
            });
          }
        }
      });
    });
  }

  getEditDisableDesc(): string | undefined {
    if (
      this.selection &&
      this.selection.hasSingleSelection &&
      !this.selection.first().sources.orchestrator
    ) {
      return this.i18n('Host editing is disabled because the host is not managed by Orchestrator.');
    }
    return undefined;
  }

  deleteAction() {
    const hostname = this.selection.first().hostname;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'Host',
        itemNames: [hostname],
        actionDescription: 'delete',
        submitActionObservable: () =>
          this.taskWrapper.wrapTaskAroundCall({
            task: new FinishedTask('host/delete', { hostname: hostname }),
            call: this.hostService.delete(hostname)
          })
      }
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
