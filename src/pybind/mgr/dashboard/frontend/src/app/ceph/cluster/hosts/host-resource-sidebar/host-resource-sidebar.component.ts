import {
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';

import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { HostService, HostModalRef } from '~/app/shared/api/host.service';
import { HostActionService } from '~/app/shared/services/host-action.service';
import { Host, HostStatusConfig, STATUS_MAP, getStatus } from '~/app/shared/models/host.interface';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { ActionLabels, ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
@Component({
  selector: 'cd-host-sidebar',
  templateUrl: './host-resource-sidebar.component.html',
  styleUrls: ['./host-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class HostSidebarComponent implements OnInit, OnDestroy {
  readonly HostStatus = HostStatus;

  @ViewChild('maintenanceConfirmTpl', { static: true })
  maintenanceConfirmTpl!: TemplateRef<any>;

  private sub = new Subscription();
  public readonly basePath = '/hosts';
  hostname = '';
  hostLabels: string[] = [];
  hostStatus: HostStatus | string | null = null;
  hostStatusMap: HostStatusConfig | null = null;
  hostActions: CdTableAction[] = [];
  hostSelection = new CdTableSelection();
  sidebarItems: SidebarItem[] = [];
  permissions: Permissions;
  modalRef?: HostModalRef;
  isExecuting = false;
  errorMessage: string[] = [];
  enableMaintenanceBtn = false;
  draining = false;
  orchStatus!: OrchestratorStatus;

  messages = {
    nonOrchHost: $localize`The feature is disabled because the selected host is not managed by Orchestrator.`
  };

  actionOrchFeatures: Record<string, OrchestratorFeature[]> = {
    [ActionLabels.EDIT]: [
      OrchestratorFeature.HOST_LABEL_ADD,
      OrchestratorFeature.HOST_LABEL_REMOVE
    ],
    [ActionLabels.REMOVE]: [OrchestratorFeature.HOST_REMOVE],
    [ActionLabels.MAINTENANCE]: [
      OrchestratorFeature.HOST_MAINTENANCE_ENTER,
      OrchestratorFeature.HOST_MAINTENANCE_EXIT
    ],
    [ActionLabels.DRAIN]: [OrchestratorFeature.HOST_DRAIN]
  };

  constructor(
    private route: ActivatedRoute,
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private hostActionService: HostActionService,
    private actionLabels: ActionLabelsI18n,
    private orchService: OrchestratorService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.hostname = pm.get('hostname') ?? '';
        this.buildSidebarItems(this.permissions);
        this.loadHostMetadata();
      })
    );

    this.sub.add(
      this.orchService.status().subscribe((orchStatus) => {
        this.orchStatus = orchStatus;
        this.buildHostActions();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(permissions: Permissions): void {
    const items: SidebarItem[] = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.hostname, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Storage Devices`,
        route: [this.basePath, this.hostname, 'storage-devices'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (permissions.hosts?.read) {
      items.push({
        label: $localize`Daemons`,
        route: [this.basePath, this.hostname, 'daemons'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    if (permissions.grafana?.read) {
      items.push({
        label: $localize`Performance`,
        route: [this.basePath, this.hostname, 'performance'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    this.sidebarItems = items;
  }

  private loadHostMetadata(): void {
    if (!this.hostname) {
      this.hostLabels = [];
      this.hostStatus = null;
      this.hostStatusMap = null;
      return;
    }

    this.sub.add(
      this.hostService.getAllHosts().subscribe((hosts: Host[]) => {
        const host = hosts.find((item) => item.hostname === this.hostname);
        this.hostLabels = Array.isArray(host?.labels) ? host.labels : [];
        this.hostStatus = getStatus(host);
        this.enableMaintenanceBtn = this.hostStatus === HostStatus.MAINTENANCE;
        this.hostStatusMap = STATUS_MAP[this.hostStatus];
        this.draining = this.hostLabels.includes('_no_schedule');
        this.hostSelection.selected = host ? [host] : [];
        this.buildHostActions();
      })
    );
  }

  private buildHostActions(): void {
    this.hostActions = [
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction(),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.EDIT, selection)
      },
      {
        name: this.draining ? this.actionLabels.STOP_DRAIN : this.actionLabels.START_DRAIN,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostDrain(this.draining),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.DRAIN, selection)
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.REMOVE, selection)
      },
      {
        name: this.enableMaintenanceBtn
          ? this.actionLabels.EXIT_MAINTENANCE
          : this.actionLabels.ENTER_MAINTENANCE,
        permission: 'update',
        icon: this.enableMaintenanceBtn ? Icons.exit : Icons.enter,
        click: () => this.hostMaintenance(),
        disable: (selection: CdTableSelection) =>
          this.getDisable(ActionLabels.MAINTENANCE, selection) || this.isExecuting
      }
    ];
  }

  getVisibleHostActions(): CdTableAction[] {
    return this.hostActions.filter(
      (action) => !action.visible || action.visible(this.hostSelection)
    );
  }

  isHostActionDisabled(action: CdTableAction): boolean {
    return !!action.disable?.(this.hostSelection);
  }

  runHostAction(action: CdTableAction): void {
    if (this.isHostActionDisabled(action)) {
      return;
    }
    action.click?.();
  }

  getDisable(action: string, selection: CdTableSelection): boolean | string {
    return this.hostService.getDisable(
      action,
      selection,
      this.orchStatus as OrchestratorStatus,
      this.actionOrchFeatures,
      this.messages.nonOrchHost,
      [ActionLabels.EDIT, ActionLabels.REMOVE, ActionLabels.MAINTENANCE, ActionLabels.DRAIN]
    );
  }

  editAction() {
    const host = this.hostSelection.first();
    if (!host) {
      return;
    }

    this.hostActionService.openEditModal(host, () => {
      this.loadHostMetadata();
    });
  }

  hostMaintenance() {
    const host = this.hostSelection.first();
    if (!host) {
      return;
    }

    this.hostActionService.hostMaintenance(
      host,
      this.maintenanceConfirmTpl,
      (isExecuting: boolean) => (this.isExecuting = isExecuting),
      (errorMessage: string[]) => (this.errorMessage = errorMessage),
      () => this.loadHostMetadata(),
      () => this.loadHostMetadata(),
      (modalRef: HostModalRef) => {
        this.modalRef = modalRef;
      }
    );
  }

  hostDrain(stop = false) {
    const host = this.hostSelection.first();
    if (!host) {
      return;
    }

    this.hostActionService.hostDrain(host, stop, () => {
      this.loadHostMetadata();
    });
  }

  deleteAction() {
    const host = this.hostSelection.first();
    if (!host) {
      return;
    }

    this.modalRef = this.hostActionService.deleteAction(host.hostname);
  }
}
