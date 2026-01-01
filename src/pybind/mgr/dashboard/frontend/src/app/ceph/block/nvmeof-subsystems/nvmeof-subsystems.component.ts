import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { Icons } from '~/app/shared/enum/icons.enum';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';

const BASE_URL = 'block/nvmeof/subsystems';
const DEFAULT_PLACEHOLDER = $localize`Enter group name`;

@Component({
  selector: 'cd-nvmeof-subsystems',
  templateUrl: './nvmeof-subsystems.component.html',
  styleUrls: ['./nvmeof-subsystems.component.scss']
})
export class NvmeofSubsystemsComponent extends ListWithDetails implements OnInit {
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  subsystems: NvmeofSubsystem[] = [];
  subsystemsColumns: any;
  permissions: Permissions;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  subsystemDetails: any[];
  gwGroups: GroupsComboboxItem[] = [];
  group: string = null;
  gwGroupsEmpty: boolean = false;
  gwGroupPlaceholder: string = DEFAULT_PLACEHOLDER;

  constructor(
    private nvmeofService: NvmeofService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private route: ActivatedRoute,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.route.queryParams.subscribe((params) => {
      if (params?.['group']) this.onGroupSelection({ content: params?.['group'] });
    });
    this.setGatewayGroups();
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
        click: () =>
          this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.CREATE] } }], {
            queryParams: { group: this.group }
          }),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.group
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteSubsystemModal()
      }
    ];
  }

  // Subsystems
  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getSubsystems() {
    if (this.group) {
      this.nvmeofService
        .listSubsystems(this.group)
        .subscribe((subsystems: NvmeofSubsystem[] | NvmeofSubsystem) => {
          if (Array.isArray(subsystems)) this.subsystems = subsystems;
          else this.subsystems = [subsystems];
        });
    } else {
      this.subsystems = [];
    }
  }

  deleteSubsystemModal() {
    const subsystem = this.selection.first();
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Subsystem`,
      impact: DeletionImpact.high,
      bodyTemplate: this.deleteTpl,
      itemNames: [subsystem.nqn],
      actionDescription: 'delete',
      bodyContext: {
        deletionMessage: $localize`Deleting <strong>${subsystem.nqn}</strong> will remove all associated configurations and resources. Dependent services may stop working. This action cannot be undone.`
      },
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/subsystem/delete', { nqn: subsystem.nqn }),
          call: this.nvmeofService.deleteSubsystem(subsystem.nqn, this.group)
        })
    });
  }

  onGroupSelection(selected: GroupsComboboxItem) {
    selected.selected = true;
    this.group = selected.content;
    this.getSubsystems();
  }

  onGroupClear() {
    this.group = null;
    this.getSubsystems();
  }

  setGatewayGroups() {
    this.nvmeofService.listGatewayGroups().subscribe((response: CephServiceSpec[][]) => {
      if (response?.[0]?.length) {
        this.gwGroups = this.nvmeofService.formatGwGroupsList(response);
      } else this.gwGroups = [];
      // Select first group if no group is selected
      if (!this.group && this.gwGroups.length) {
        this.onGroupSelection(this.gwGroups[0]);
        this.gwGroupsEmpty = false;
        this.gwGroupPlaceholder = DEFAULT_PLACEHOLDER;
      } else {
        this.gwGroupsEmpty = true;
        this.gwGroupPlaceholder = $localize`No groups available`;
      }
    });
  }
}
