import {
  AfterViewInit,
  Component,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  QueryList,
  TemplateRef,
  ViewChild,
  ViewChildren
} from '@angular/core';

import _ from 'lodash';
import { Observable, Subscription } from 'rxjs';
import { take } from 'rxjs/operators';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { DaemonService } from '~/app/shared/api/daemon.service';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Daemon } from '~/app/shared/models/daemon.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-service-daemon-list',
  templateUrl: './service-daemon-list.component.html',
  styleUrls: ['./service-daemon-list.component.scss']
})
export class ServiceDaemonListComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;

  @ViewChild('listTpl', { static: true })
  listTpl: TemplateRef<any>;

  @ViewChildren('daemonsTable')
  daemonsTableTpls: QueryList<TemplateRef<TableComponent>>;

  @Input()
  serviceName?: string;

  @Input()
  hostname?: string;

  @Input()
  flag?: string;

  icons = Icons;

  daemons: Daemon[] = [];
  services: Array<CephServiceSpec> = [];
  columns: CdTableColumn[] = [];
  serviceColumns: CdTableColumn[] = [];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permissions: Permissions;

  hasOrchestrator = false;
  showDocPanel = false;

  private daemonsTable: TableComponent;
  private daemonsTableTplsSub: Subscription;
  private serviceSub: Subscription;

  constructor(
    private hostService: HostService,
    private cephServiceService: CephServiceService,
    private orchService: OrchestratorService,
    private relativeDatePipe: RelativeDatePipe,
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private daemonService: DaemonService,
    private notificationService: NotificationService
  ) {}

  ngOnInit() {
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        permission: 'update',
        icon: Icons.start,
        click: () => this.daemonAction('start'),
        name: this.actionLabels.START,
        disable: () => this.actionDisabled('start')
      },
      {
        permission: 'update',
        icon: Icons.stop,
        click: () => this.daemonAction('stop'),
        name: this.actionLabels.STOP,
        disable: () => this.actionDisabled('stop')
      },
      {
        permission: 'update',
        icon: Icons.restart,
        click: () => this.daemonAction('restart'),
        name: this.actionLabels.RESTART,
        disable: () => this.actionDisabled('restart')
      },
      {
        permission: 'update',
        icon: Icons.deploy,
        click: () => this.daemonAction('redeploy'),
        name: this.actionLabels.REDEPLOY,
        disable: () => this.actionDisabled('redeploy')
      }
    ];
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 2,
        filterable: true
      },
      {
        name: $localize`Daemon type`,
        prop: 'daemon_type',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Daemon ID`,
        prop: 'daemon_id',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Container ID`,
        prop: 'container_id',
        flexGrow: 2,
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: $localize`Container Image name`,
        prop: 'container_image_name',
        flexGrow: 3,
        filterable: true
      },
      {
        name: $localize`Container Image ID`,
        prop: 'container_image_id',
        flexGrow: 2,
        filterable: true,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: $localize`Version`,
        prop: 'version',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Status`,
        prop: 'status_desc',
        flexGrow: 1,
        filterable: true,
        cellTemplate: this.statusTpl
      },
      {
        name: $localize`Last Refreshed`,
        prop: 'last_refresh',
        pipe: this.relativeDatePipe,
        flexGrow: 1
      },
      {
        name: $localize`Daemon Events`,
        prop: 'events',
        flexGrow: 5,
        cellTemplate: this.listTpl
      }
    ];

    this.serviceColumns = [
      {
        name: $localize`Service Name`,
        prop: 'service_name',
        flexGrow: 2,
        filterable: true
      },
      {
        name: $localize`Service Type`,
        prop: 'service_type',
        flexGrow: 1,
        filterable: true
      },
      {
        name: $localize`Service Events`,
        prop: 'events',
        flexGrow: 5,
        cellTemplate: this.listTpl
      }
    ];

    this.orchService.status().subscribe((data: { available: boolean }) => {
      this.hasOrchestrator = data.available;
      this.showDocPanel = !data.available;
    });
  }

  ngOnChanges() {
    if (!_.isUndefined(this.daemonsTable)) {
      this.daemonsTable.reloadData();
    }
  }

  ngAfterViewInit() {
    this.daemonsTableTplsSub = this.daemonsTableTpls.changes.subscribe(
      (tableRefs: QueryList<TableComponent>) => {
        this.daemonsTable = tableRefs.first;
      }
    );
  }

  ngOnDestroy() {
    if (this.daemonsTableTplsSub) {
      this.daemonsTableTplsSub.unsubscribe();
    }
    if (this.serviceSub) {
      this.serviceSub.unsubscribe();
    }
  }

  getStatusClass(row: Daemon): string {
    return _.get(
      {
        '-1': 'badge-danger',
        '0': 'badge-warning',
        '1': 'badge-success'
      },
      row.status,
      'badge-dark'
    );
  }

  getDaemons(context: CdTableFetchDataContext) {
    let observable: Observable<Daemon[]>;
    if (this.hostname) {
      observable = this.hostService.getDaemons(this.hostname);
    } else if (this.serviceName) {
      observable = this.cephServiceService.getDaemons(this.serviceName);
    } else {
      this.daemons = [];
      return;
    }
    observable.subscribe(
      (daemons: Daemon[]) => {
        this.daemons = daemons;
        this.sortDaemonEvents();
      },
      () => {
        this.daemons = [];
        context.error();
      }
    );
  }

  sortDaemonEvents() {
    this.daemons.forEach((daemon: any) => {
      daemon.events?.sort((event1: any, event2: any) => {
        return new Date(event2.created).getTime() - new Date(event1.created).getTime();
      });
    });
  }
  getServices(context: CdTableFetchDataContext) {
    this.serviceSub = this.cephServiceService.list(this.serviceName).subscribe(
      (services: CephServiceSpec[]) => {
        this.services = services;
      },
      () => {
        this.services = [];
        context.error();
      }
    );
  }

  trackByFn(_index: any, item: any) {
    return item.created;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  daemonAction(actionType: string) {
    this.daemonService
      .action(this.selection.first()?.daemon_name, actionType)
      .pipe(take(1))
      .subscribe({
        next: (resp) => {
          this.notificationService.show(
            NotificationType.success,
            `Daemon ${actionType} scheduled`,
            resp.body.toString()
          );
        },
        error: (resp) => {
          this.notificationService.show(
            NotificationType.error,
            'Daemon action failed',
            resp.body.toString()
          );
        }
      });
  }

  actionDisabled(actionType: string) {
    if (this.selection?.hasSelection) {
      const daemon = this.selection.selected[0];
      if (daemon.daemon_type === 'mon' || daemon.daemon_type === 'mgr') {
        return true; // don't allow actions on mon and mgr, dashboard requires them.
      }
      switch (actionType) {
        case 'start':
          if (daemon.status_desc === 'running') {
            return true;
          }
          break;
        case 'stop':
          if (daemon.status_desc === 'stopped') {
            return true;
          }
          break;
      }
      return false;
    }
    return true; // if no selection then disable everything
  }
}
