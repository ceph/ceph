import { Component, Input, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons, ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofListener } from '~/app/shared/models/nvmeof';
import { Host } from '~/app/shared/models/host.interface';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-nvmeof-listeners-list',
  templateUrl: './nvmeof-listeners-list.component.html',
  styleUrls: ['./nvmeof-listeners-list.component.scss'],
  standalone: false
})
export class NvmeofListenersListComponent implements OnInit {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;

  listenerColumns: CdTableColumn[];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  listeners: NvmeofListener[];
  hasAvailableNodes = true;
  iconType = ICON_TYPE;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    // If inputs are not provided, try to get from route params (when used as routed component)
    if (!this.subsystemNQN || !this.group) {
      this.route.parent?.params.subscribe((params) => {
        if (params['subsystem_nqn']) {
          this.subsystemNQN = params['subsystem_nqn'];
        }
        if (this.subsystemNQN && this.group) {
          this.listListeners();
        }
      });
      this.route.queryParams.subscribe((qp) => {
        if (qp['group']) {
          this.group = qp['group'];
        }
        if (this.subsystemNQN && this.group) {
          this.listListeners();
        }
      });
    }

    this.listenerColumns = [
      {
        name: $localize`Name`,
        prop: 'host_name'
      },
      {
        name: $localize`Transport`,
        prop: 'trtype'
      },
      {
        name: $localize`Address`,
        prop: 'full_addr',
        cellTransformation: CellTemplate.copy
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate([{ outlets: { modal: [URLVerbs.ADD, 'listener'] } }], {
            queryParams: { group: this.group },
            relativeTo: this.route.parent
          }),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () =>
          !this.hasAvailableNodes ? $localize`All available nodes already have listeners` : false
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteListenerModal()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listListeners() {
    this.nvmeofService
      .listListeners(this.subsystemNQN, this.group)
      .subscribe((listResponse: NvmeofListener[]) => {
        this.listeners = listResponse.map((listener, index) => {
          listener['id'] = index;
          listener['full_addr'] = `${listener.traddr}:${listener.trsvcid}`;
          return listener;
        });
        this.checkAvailableNodes();
      });
  }

  checkAvailableNodes() {
    if (!this.group) return;
    this.nvmeofService.getHostsForGroup(this.group).subscribe({
      next: (allHosts: Host[]) => {
        const listenerHostNames = new Set((this.listeners || []).map((l) => l.host_name));
        this.hasAvailableNodes = allHosts.some((h) => !listenerHostNames.has(h.hostname));
      },
      error: () => {
        this.hasAvailableNodes = true;
      }
    });
  }

  deleteListenerModal() {
    const listener = this.selection.first();
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Listener`,
      actionDescription: 'delete',
      infoMessage: $localize`This action will delete listener despite any active connections.`,
      itemNames: [
        $localize`listener` + ' ' + `${listener.host_name} (${listener.traddr}:${listener.trsvcid})`
      ],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/listener/delete', {
            nqn: this.subsystemNQN,
            host_name: listener.host_name
          }),
          call: this.nvmeofService.deleteListener(
            this.subsystemNQN,
            this.group,
            listener.host_name,
            listener.traddr,
            listener.trsvcid
          )
        })
    });
  }
}
