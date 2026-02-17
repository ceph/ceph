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
import { Observable, Subject, Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';

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
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NvmeofGatewayNodeAddModalComponent } from './nvmeof-gateway-node-add-modal/nvmeof-gateway-node-add-modal.component';

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
  @Input() mode: 'selector' | 'details' = NvmeofGatewayNodeMode.SELECTOR;

  usedHostnames: Set<string> = new Set();
  serviceSpec: CephServiceSpec | undefined;
  hasAvailableHosts = false;

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
  count = 0;
  orchStatus: OrchestratorStatus | undefined;
  private destroy$ = new Subject<void>();
  private sub: Subscription | undefined;

  constructor(
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService,
    private route: ActivatedRoute,
    private modalService: ModalCdsService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit(): void {
    const routeData = this.route.snapshot.data;
    if (routeData?.['mode']) {
      this.mode = routeData['mode'];
    }

    this.selectionType = this.mode === NvmeofGatewayNodeMode.SELECTOR ? 'multiClick' : 'single';

    if (this.mode === NvmeofGatewayNodeMode.DETAILS) {
      this.route.parent?.params.subscribe((params: any) => {
        this.groupName = params.group;
      });
      this.setTableActions();
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

  private setTableActions() {
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.addGateway(),
        name: $localize`Add`,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => (!this.hasAvailableHosts ? $localize`No available nodes to add` : false)
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

  addGateway(): void {
    const modalRef = this.modalService.show(NvmeofGatewayNodeAddModalComponent, {
      groupName: this.groupName,
      usedHostnames: Array.from(this.usedHostnames),
      serviceSpec: this.serviceSpec
    });

    modalRef.gatewayAdded.subscribe(() => {
      this.table.refreshBtn();
    });
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

    const fetchData$: Observable<any> =
      this.mode === NvmeofGatewayNodeMode.DETAILS
        ? this.nvmeofService.fetchHostsAndGroups()
        : this.nvmeofService.getAvailableHosts(this.tableContext?.toParams());

    this.sub = fetchData$
      .pipe(
        finalize(() => {
          this.isLoadingHosts = false;
        })
      )
      .subscribe({
        next: (result: any) => {
          if (this.mode === NvmeofGatewayNodeMode.DETAILS) {
            this.processDetailsData(result.groups, result.hosts);
          } else {
            this.hosts = result;
            this.count = this.hosts.length;
            this.hostsLoaded.emit(this.count);
          }
        },
        error: () => context?.error()
      });
  }

  private processDetailsData(groups: any[][], hostList: Host[]) {
    const groupList = groups?.[0] ?? [];

    const allUsedHostnames = new Set<string>();
    groupList.forEach((group: CephServiceSpec) => {
      const hosts = group.placement?.hosts || (group.spec as any)?.placement?.hosts || [];
      hosts.forEach((hostname: string) => allUsedHostnames.add(hostname));
    });

    this.usedHostnames = allUsedHostnames;

    // Check if there are any available hosts globally (not used by any group)
    this.hasAvailableHosts = (hostList || []).some(
      (host: Host) => !this.usedHostnames.has(host.hostname)
    );
    this.setTableActions();

    const currentGroup = groupList.find((group: CephServiceSpec) => {
      return (
        group.spec?.group === this.groupName ||
        group.service_id === `nvmeof.${this.groupName}` ||
        group.service_id.endsWith(`.${this.groupName}`)
      );
    });

    this.serviceSpec = currentGroup as CephServiceSpec;

    if (!this.serviceSpec) {
      this.hosts = [];
    } else {
      const placementHosts =
        this.serviceSpec.placement?.hosts || (this.serviceSpec.spec as any)?.placement?.hosts || [];
      const currentGroupHosts = new Set<string>(placementHosts);

      this.hosts = (hostList || []).filter((host: Host) => {
        return currentGroupHosts.has(host.hostname);
      });
    }

    this.count = this.hosts.length;
    this.hostsLoaded.emit(this.count);
  }
}
