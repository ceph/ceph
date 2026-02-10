import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Subject, Subscription } from 'rxjs';
import { finalize, mergeMap } from 'rxjs/operators';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NvmeofGatewayNodeMode } from '~/app/shared/enum/nvmeof.enum';

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
  @Input() groupName: string | undefined;
  @Input() mode: NvmeofGatewayNodeMode = NvmeofGatewayNodeMode.SELECTOR;

  usedHostnames: Set<string> = new Set();
  serviceSpec: CephServiceSpec | undefined;

  permission: Permission;
  columns: CdTableColumn[] = [];
  hosts: Host[] = [];
  isLoadingHosts = false;
  tableActions: CdTableAction[] = [];
  selectionType: 'single' | 'multiClick' | 'none' = 'single';

  selection = new CdTableSelection();
  icons = Icons;
  HostStatus = HostStatus;
  private tableContext: CdTableFetchDataContext | undefined;
  totalHostCount = 5;
  orchStatus: OrchestratorStatus | undefined;
  private destroy$ = new Subject<void>();
  private sub: Subscription | undefined;

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private orchService: OrchestratorService,
    private nvmeofService: NvmeofService,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit(): void {
    this.route.data.subscribe((data) => {
      if (data?.['mode']) {
        this.mode = data['mode'];
      }
    });

    this.selectionType = this.mode === NvmeofGatewayNodeMode.SELECTOR ? 'multiClick' : 'single';

    if (this.mode === NvmeofGatewayNodeMode.DETAILS) {
      this.route.parent?.params.subscribe((params: { group: string }) => {
        this.groupName = params.group;
      });
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
    }

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
    // TODO
  }

  removeGateway(): void {
    // TODO
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    if (this.sub) {
      this.sub.unsubscribe();
    }
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
    this.selectionChange.emit(selection);
  }

  getSelectedHostnames(): string[] {
    return this.selection.selected.map((host: Host) => host.hostname);
  }

  getHosts(context: CdTableFetchDataContext): void {
    this.tableContext =
      context || this.tableContext || new CdTableFetchDataContext(() => undefined);
    if (this.isLoadingHosts) {
      return;
    }
    this.isLoadingHosts = true;

    if (this.sub) {
      this.sub.unsubscribe();
    }

    const fetchData$ =
      this.mode === NvmeofGatewayNodeMode.DETAILS
        ? this.nvmeofService.fetchHostsAndGroups()
        : forkJoin({
            groups: this.nvmeofService.listGatewayGroups(),
            hosts: this.orchService.status().pipe(
              mergeMap((orchStatus: OrchestratorStatus) => {
                this.orchStatus = orchStatus;
                const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
                return this.hostService.list(
                  this.tableContext?.toParams(),
                  factsAvailable.toString()
                );
              })
            )
          });

    this.sub = fetchData$
      .pipe(
        finalize(() => {
          this.isLoadingHosts = false;
        })
      )
      .subscribe({
        next: (result: any) => {
          this.mode === NvmeofGatewayNodeMode.DETAILS
            ? this.processHostsForDetailsMode(result.groups, result.hosts)
            : this.processHostsForSelectorMode(result.groups, result.hosts);
        },
        error: () => context?.error()
      });
  }

  /**
   * Selector Mode: Used in 'Add/Create' forms.
   * Filters the entire cluster inventory to show only **available** candidates
   * (excluding nodes that are already part of a gateway group).
   */
  private processHostsForSelectorMode(groups: CephServiceSpec[][] = [[]], hostList: Host[] = []) {
    const usedHosts = new Set<string>();
    (groups?.[0] ?? []).forEach((group: CephServiceSpec) => {
      group.placement?.hosts?.forEach((hostname: string) => usedHosts.add(hostname));
    });
    this.usedHostnames = usedHosts;

    this.hosts = (hostList || []).filter((host: Host) => !this.usedHostnames.has(host.hostname));

    this.updateCount();
  }

  /**
   * Details Mode: Used in 'Details' views.
   * Filters specifically for the nodes that are **configured members**
   * of the current gateway group, regardless of their status.
   */
  private processHostsForDetailsMode(groups: any[][], hostList: Host[]) {
    const groupList = groups?.[0] ?? [];
    const currentGroup: CephServiceSpec | undefined = groupList.find(
      (group: CephServiceSpec) => group.spec?.group === this.groupName
    );

    if (!currentGroup) {
      this.hosts = [];
    } else {
      const placementHosts =
        currentGroup.placement?.hosts || (currentGroup.spec as any)?.placement?.hosts || [];
      const currentGroupHosts = new Set<string>(placementHosts);

      this.hosts = (hostList || []).filter((host: Host) => {
        return currentGroupHosts.has(host.hostname);
      });
    }

    this.serviceSpec = currentGroup;
    this.updateCount();
  }

  private updateCount(): void {
    this.totalHostCount = this.hosts.length;
    this.hostsLoaded.emit(this.totalHostCount);
  }
}
