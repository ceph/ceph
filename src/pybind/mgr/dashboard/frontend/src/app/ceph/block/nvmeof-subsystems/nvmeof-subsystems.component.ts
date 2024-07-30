import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-subsystems',
  templateUrl: './nvmeof-subsystems.component.html',
  styleUrls: ['./nvmeof-subsystems.component.scss']
})
export class NvmeofSubsystemsComponent extends ListWithDetails implements OnInit {
  subsystems: NvmeofSubsystem[] = [];
  subsystemsColumns: any;
  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  subsystemDetails: any[];

  constructor(
    private nvmeofService: NvmeofService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.subsystemsColumns = [
      {
        name: $localize`NQN`,
        prop: 'nqn'
      },
      {
        name: $localize`# Namespaces`,
        prop: 'namespace_count'
      },
      {
        name: $localize`# Maximum Allowed Namespaces`,
        prop: 'max_namespaces'
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.CREATE] } }]),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () =>
          this.router.navigate([
            BASE_URL,
            {
              outlets: {
                modal: [
                  URLVerbs.EDIT,
                  this.selection.first().nqn,
                  this.selection.first().max_namespaces
                ]
              }
            }
          ])
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteSubsystemModal()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getSubsystems() {
    this.nvmeofService
      .listSubsystems()
      .subscribe((subsystems: NvmeofSubsystem[] | NvmeofSubsystem) => {
        if (Array.isArray(subsystems)) this.subsystems = subsystems;
        else this.subsystems = [subsystems];
      });
  }

  deleteSubsystemModal() {
    const subsystem = this.selection.first();
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Subsystem',
      itemNames: [subsystem.nqn],
      actionDescription: 'delete',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/subsystem/delete', { nqn: subsystem.nqn }),
          call: this.nvmeofService.deleteSubsystem(subsystem.nqn)
        })
    });
  }
}
