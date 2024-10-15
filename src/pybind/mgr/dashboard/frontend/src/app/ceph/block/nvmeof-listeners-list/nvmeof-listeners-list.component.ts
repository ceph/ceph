import { Component, Input, OnChanges, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofListener } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-listeners-list',
  templateUrl: './nvmeof-listeners-list.component.html',
  styleUrls: ['./nvmeof-listeners-list.component.scss']
})
export class NvmeofListenersListComponent implements OnInit, OnChanges {
  @Input()
  subsystemNQN: string;

  listenerColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  listeners: NvmeofListener[];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private router: Router
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.listenerColumns = [
      {
        name: $localize`Host`,
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
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate([
            BASE_URL,
            { outlets: { modal: [URLVerbs.CREATE, this.subsystemNQN, 'listener'] } }
          ]),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteListenerModal()
      }
    ];
  }

  ngOnChanges() {
    this.listListeners();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listListeners() {
    this.nvmeofService
      .listListeners(this.subsystemNQN)
      .subscribe((listResponse: NvmeofListener[]) => {
        this.listeners = listResponse.map((listener, index) => {
          listener['id'] = index;
          listener['full_addr'] = `${listener.traddr}:${listener.trsvcid}`;
          return listener;
        });
      });
  }

  deleteListenerModal() {
    const listener = this.selection.first();
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Listener',
      actionDescription: 'delete',
      itemNames: [`listener ${listener.host_name} (${listener.traddr}:${listener.trsvcid})`],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/listener/delete', {
            nqn: this.subsystemNQN,
            host_name: listener.host_name
          }),
          call: this.nvmeofService.deleteListener(
            this.subsystemNQN,
            listener.host_name,
            listener.traddr,
            listener.trsvcid
          )
        })
    });
  }
}
