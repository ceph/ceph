import {
  Component,
  EventEmitter,
  OnDestroy,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { forkJoin, Subject } from 'rxjs';
import { map, mergeMap, takeUntil } from 'rxjs/operators';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permission } from '~/app/shared/models/permissions';

import { Host } from '~/app/shared/models/host.interface';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';


@Component({
  selector: 'cd-nvmeof-gateway-node',
  templateUrl: './nvmeof-gateway-node.component.html',
  styleUrls: ['./nvmeof-gateway-node.component.scss'],
  standalone: false
})
export class NvmeofGatewayNodeComponent implements OnInit, OnDestroy {
  @ViewChild(TableComponent, { static: true })
  table!: TableComponent;

  @ViewChild('hostNameTpl', { static: true })
  hostNameTpl!: TemplateRef<any>;

  @ViewChild('statusTpl', { static: true })
  statusTpl!: TemplateRef<any>;

  @ViewChild('addrTpl', { static: true })
  addrTpl!: TemplateRef<any>;

  @ViewChild('labelsTpl', { static: true })
  labelsTpl!: TemplateRef<any>;

  @Output() selectionChange = new EventEmitter<CdTableSelection>();
  @Output() hostsLoaded = new EventEmitter<number>();

  usedHostnames: Set<string> = new Set();

  permission: Permission;
  columns: CdTableColumn[] = [];
  hosts: Host[] = [];
  isLoadingHosts = false;
  tableActions!: CdTableAction[];

  selection = new CdTableSelection();
  icons = Icons;
  HostStatus = HostStatus;
  private tableContext: CdTableFetchDataContext = null;
  count = 5;
  orchStatus: OrchestratorStatus;
  private destroy$ = new Subject<void>();

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private orchService: OrchestratorService,
    private nvmeofService: NvmeofService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit(): void {
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.addGateway(),
        name: $localize`Add`,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeGateway(),
        name: $localize`Remove`,
        disable: (selection: CdTableSelection) => !selection.hasSelection
      }
    ];

    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1,
        cellTemplate: this.hostNameTpl
      },
      {
        name: $localize`IP address`,
        prop: 'addr',
        flexGrow: 0.8,
        cellTemplate: this.addrTpl
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8,
        cellTemplate: this.statusTpl
      },
      {
        name: $localize`Labels (tags)`,

        prop: 'labels',
        flexGrow: 1,
        cellTemplate: this.labelsTpl
      }
    ];
  }

  addGateway(): void {
    // TODO: Logic to open add gateway modal
  }

  removeGateway(): void {
    // TODO: Logic to remove gateway
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
    this.selectionChange.emit(selection);
  }

  getSelectedHostnames(): string[] {
    return this.selection.selected.map((host: Host) => host.hostname);
  }

  getHosts(context: CdTableFetchDataContext): void {
    if (context !== null) {
      this.tableContext = context;
    }
    if (this.tableContext == null) {
      this.tableContext = new CdTableFetchDataContext(() => undefined);
    }
    if (this.isLoadingHosts) {
      return;
    }
    this.isLoadingHosts = true;

    forkJoin([this.buildUsedHostsObservable(), this.buildHostListObservable()])
      .pipe(takeUntil(this.destroy$))
      .subscribe(
        ([usedHostnames, hostList]: [Set<string>, Host[]]) =>
          this.processHostResults(usedHostnames, hostList),
        () => {
          this.isLoadingHosts = false;
          context.error();
        }
      );
  }

  private buildUsedHostsObservable() {
    return this.nvmeofService.listGatewayGroups().pipe(
      map((groups: CephServiceSpec[][]) => {
        const usedHosts = new Set<string>();
        const groupList = groups?.[0] ?? [];
        groupList.forEach((group: CephServiceSpec) => {
          const hosts = group.placement?.hosts || [];
          hosts.forEach((hostname: string) => usedHosts.add(hostname));
        });
        return usedHosts;
      })
    );
  }

  private buildHostListObservable() {
    return this.orchService.status().pipe(
      mergeMap((orchStatus) => {
        this.orchStatus = orchStatus;
        const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
        return this.hostService.list(this.tableContext?.toParams(), factsAvailable.toString());
      })
    );
  }

  private processHostResults(usedHostnames: Set<string>, hostList: Host[]) {
    this.usedHostnames = usedHostnames;
    this.hosts = (hostList || [])
      .map((host: Host) => ({
        ...host,
        status: host.status || HostStatus.AVAILABLE
      }))
      .filter((host: Host) => {
        const isNotUsed = !this.usedHostnames.has(host.hostname);
        const status = host.status || HostStatus.AVAILABLE;
        const isAvailable = status === HostStatus.AVAILABLE || status === HostStatus.RUNNING;
        return isNotUsed && isAvailable;
      });

    this.isLoadingHosts = false;
    this.count = this.hosts.length;
    this.hostsLoaded.emit(this.count);
  }

  checkHostsFactsAvailable(): boolean {
    return this.hostService.checkHostsFactsAvailable(this.orchStatus);
  }
}
