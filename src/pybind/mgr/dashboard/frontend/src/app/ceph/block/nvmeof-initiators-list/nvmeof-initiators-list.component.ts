import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemInitiator } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-initiators-list',
  templateUrl: './nvmeof-initiators-list.component.html',
  styleUrls: ['./nvmeof-initiators-list.component.scss']
})
export class NvmeofInitiatorsListComponent implements OnInit {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;

  @ViewChild('hostTpl', { static: true })
  hostTpl: TemplateRef<any>;

  initiatorColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  initiators: NvmeofSubsystemInitiator[] = [];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService,
    private modalService: ModalCdsService,
    private router: Router,
    private taskWrapper: TaskWrapperService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.initiatorColumns = [
      {
        name: $localize`Initiator`,
        prop: 'nqn',
        cellTemplate: this.hostTpl
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate(
            [BASE_URL, { outlets: { modal: [URLVerbs.ADD, this.subsystemNQN, 'initiator'] } }],
            { queryParams: { group: this.group } }
          ),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeInitiatorModal(),
        disable: () => !this.selection.hasSelection,
        canBePrimary: (selection: CdTableSelection) => selection.hasSelection
      }
    ];
  }

  getAllowAllHostIndex() {
    return this.selection.selected.findIndex((selected) => selected.nqn === '*');
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listInitiators() {
    this.nvmeofService
      .getInitiators(this.subsystemNQN, this.group)
      .subscribe((initiators: NvmeofSubsystemInitiator[]) => {
        this.initiators = initiators;
      });
  }

  getSelectedNQNs() {
    return this.selection.selected.map((selected) => selected.nqn);
  }

  removeInitiatorModal() {
    const hostNQNs = this.getSelectedNQNs();
    const allowAllHostIndex = this.getAllowAllHostIndex();
    const host_nqn = hostNQNs.join(',');
    let itemNames = hostNQNs;
    if (allowAllHostIndex !== -1) {
      hostNQNs.splice(allowAllHostIndex, 1);
      itemNames = [...hostNQNs, $localize`Allow any host(*)`];
    }
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'Initiator',
      itemNames,
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/initiator/remove', {
            nqn: this.subsystemNQN,
            plural: itemNames.length > 1
          }),
          call: this.nvmeofService.removeInitiators(this.subsystemNQN, {
            host_nqn,
            gw_group: this.group
          })
        })
    });
  }
}
