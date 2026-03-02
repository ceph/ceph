import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  getSubsystemAuthStatus
} from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { NvmeofSubsystemAuthType } from '~/app/shared/enum/nvmeof.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofEditHostKeyModalComponent } from '../nvmeof-edit-host-key-modal/nvmeof-edit-host-key-modal.component';

@Component({
  selector: 'cd-nvmeof-initiators-list',
  templateUrl: './nvmeof-initiators-list.component.html',
  styleUrls: ['./nvmeof-initiators-list.component.scss'],
  standalone: false
})
export class NvmeofInitiatorsListComponent implements OnInit {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;

  @ViewChild('dhchapTpl', { static: true })
  dhchapTpl: TemplateRef<any>;

  initiatorColumns: CdTableColumn[];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  initiators: NvmeofSubsystemInitiator[] = [];
  subsystem: NvmeofSubsystem;
  authStatus: string;
  authType = NvmeofSubsystemAuthType;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService,
    private modalService: ModalCdsService,
    private router: Router,
    private taskWrapper: TaskWrapperService,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    if (!this.subsystemNQN || !this.group) {
      this.route.parent?.params.subscribe((params) => {
        if (params['subsystem_nqn']) {
          this.subsystemNQN = params['subsystem_nqn'];
        }
        this.fetchIfReady();
      });
      this.route.queryParams.subscribe((qp) => {
        if (qp['group']) {
          this.group = qp['group'];
        }
        this.fetchIfReady();
      });
    } else {
      this.getSubsystem();
    }

    this.initiatorColumns = [
      {
        name: $localize`Host NQN`,
        prop: 'nqn'
      },
      {
        name: $localize`DHCHAP key`,
        prop: 'dhchap_key',
        cellTemplate: this.dhchapTpl
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate([{ outlets: { modal: [URLVerbs.ADD, 'initiator'] } }], {
            queryParams: { group: this.group },
            relativeTo: this.route.parent
          }),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => this.hasAllHostsAllowed()
      },
      {
        name: $localize`Edit host key`,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editHostKeyModal(),
        disable: () => this.selection.selected.length !== 1,
        canBePrimary: (selection: CdTableSelection) => selection.selected.length === 1
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

  private fetchIfReady() {
    if (this.subsystemNQN && this.group) {
      this.listInitiators();
      this.getSubsystem();
    }
  }

  editHostKeyModal() {
    const selected = this.selection.selected[0];
    if (!selected) return;
    this.modalService.show(NvmeofEditHostKeyModalComponent, {
      subsystemNQN: this.subsystemNQN,
      hostNQN: selected.nqn,
      group: this.group,
      dhchapKey: selected.dhchap_key || ''
    });
  }

  getAllowAllHostIndex() {
    return this.selection.selected.findIndex((selected) => selected.nqn === '*');
  }

  hasAllHostsAllowed(): boolean {
    return this.initiators.some((initiator) => initiator.nqn === '*');
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listInitiators() {
    this.nvmeofService
      .getInitiators(this.subsystemNQN, this.group)
      .subscribe((initiators: NvmeofSubsystemInitiator[]) => {
        this.initiators = initiators;
        this.updateAuthStatus();
      });
  }

  getSubsystem() {
    this.nvmeofService.getSubsystem(this.subsystemNQN, this.group).subscribe((subsystem: any) => {
      this.subsystem = subsystem;
      this.updateAuthStatus();
    });
  }

  updateAuthStatus() {
    if (this.subsystem && this.initiators) {
      this.authStatus = getSubsystemAuthStatus(this.subsystem, this.initiators);
    }
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
    const hostName = itemNames[0];
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`host`,
      impact: DeletionImpact.high,
      itemNames,
      actionDescription: 'remove',
      bodyContext: {
        deletionMessage: $localize`Removing <strong>${hostName}</strong> will disconnect it and revoke its permissions for the <strong>${this.subsystemNQN}</strong> subsystem.`
      },
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
