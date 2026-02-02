import { Component, Input, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { IopsPipe } from '~/app/shared/pipes/iops.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-namespaces-list',
  templateUrl: './nvmeof-namespaces-list.component.html',
  styleUrls: ['./nvmeof-namespaces-list.component.scss'],
  standalone: false
})
export class NvmeofNamespacesListComponent implements OnInit {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;
  @Input()
  isSubsystemView: boolean = false; // When true, hides subsystem column and shows subsystem-specific columns

  namespacesColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  namespaces: NvmeofSubsystemNamespace[];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private iopsPipe: IopsPipe,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    // If inputs are not provided, try to get from route params (when used as routed component)
    if (!this.subsystemNQN || !this.group) {
      this.isSubsystemView = true; // When loaded via route, it's the subsystem view
      this.route.parent?.params.subscribe((params) => {
        if (params['subsystem_nqn']) {
          this.subsystemNQN = params['subsystem_nqn'];
        }
        if (params['group']) {
          this.group = params['group'];
        }
        if (this.subsystemNQN && this.group) {
          this.listNamespaces();
        }
      });
    }

    this.setupColumns();
    this.setupTableActions();
  }

  setupColumns() {
    // Common columns for both views
    const commonColumns = [
      {
        name: $localize`ID`,
        prop: 'nsid'
      },
      {
        name: $localize`Pool`,
        prop: 'rbd_pool_name',
        flexGrow: 2
      },
      {
        name: $localize`Image`,
        prop: 'rbd_image_name',
        flexGrow: 3
      },
      {
        name: $localize`Image Size`,
        prop: 'rbd_image_size',
        pipe: this.dimlessBinaryPipe
      }
    ];

    // Subsystem view specific columns (inside subsystem resource page)
    const subsystemViewColumns = [
      {
        name: $localize`Block Size`,
        prop: 'block_size',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`IOPS`,
        prop: 'rw_ios_per_second',
        sortable: false,
        pipe: this.iopsPipe,
        flexGrow: 1.5
      }
    ];

    // Namespaces tab specific columns (in main gateway page)
    const namespacesTabColumns = [
      {
        name: $localize`Subsystem`,
        prop: 'subsystem_nqn',
        flexGrow: 2
      }
    ];

    if (this.isSubsystemView) {
      this.namespacesColumns = [...commonColumns, ...subsystemViewColumns];
    } else {
      this.namespacesColumns = [...commonColumns, ...namespacesTabColumns];
    }
  }

  setupTableActions() {
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate(
            [BASE_URL, { outlets: { modal: [URLVerbs.CREATE, this.subsystemNQN, 'namespace'] } }],
            { queryParams: { group: this.group } }
          ),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () =>
          this.router.navigate(
            [
              BASE_URL,
              {
                outlets: {
                  modal: [
                    URLVerbs.EDIT,
                    this.subsystemNQN,
                    'namespace',
                    this.selection.first().nsid
                  ]
                }
              }
            ],
            { queryParams: { group: this.group } }
          )
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteNamespaceModal()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listNamespaces() {
    if (this.group) {
      this.nvmeofService
        .listNamespaces(this.group)
        .subscribe((res: { namespaces: NvmeofSubsystemNamespace[] }) => {
          this.namespaces = res.namespaces || [];
        });
    } else {
      this.namespaces = [];
    }
  }

  deleteNamespaceModal() {
    const namespace = this.selection.first();
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'Namespace',
      itemNames: [namespace.nsid],
      actionDescription: 'delete',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/namespace/delete', {
            nqn: this.subsystemNQN,
            nsid: namespace.nsid
          }),
          call: this.nvmeofService.deleteNamespace(this.subsystemNQN, namespace.nsid, this.group)
        })
    });
  }
}
