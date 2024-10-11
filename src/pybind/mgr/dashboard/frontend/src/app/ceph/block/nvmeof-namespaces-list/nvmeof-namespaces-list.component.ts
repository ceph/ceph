import { Component, Input, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { IopsPipe } from '~/app/shared/pipes/iops.pipe';
import { MbpersecondPipe } from '~/app/shared/pipes/mbpersecond.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-namespaces-list',
  templateUrl: './nvmeof-namespaces-list.component.html',
  styleUrls: ['./nvmeof-namespaces-list.component.scss']
})
export class NvmeofNamespacesListComponent implements OnInit {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;

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
    private mbPerSecondPipe: MbpersecondPipe,
    private iopsPipe: IopsPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.namespacesColumns = [
      {
        name: $localize`ID`,
        prop: 'nsid'
      },
      {
        name: $localize`Bdev Name`,
        prop: 'bdev_name'
      },
      {
        name: $localize`Pool `,
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
      },
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
      },
      {
        name: $localize`R/W Throughput`,
        prop: 'rw_mbytes_per_second',
        sortable: false,
        pipe: this.mbPerSecondPipe,
        flexGrow: 1.5
      },
      {
        name: $localize`Read Throughput`,
        prop: 'r_mbytes_per_second',
        sortable: false,
        pipe: this.mbPerSecondPipe,
        flexGrow: 1.5
      },
      {
        name: $localize`Write Throughput`,
        prop: 'w_mbytes_per_second',
        sortable: false,
        pipe: this.mbPerSecondPipe,
        flexGrow: 1.5
      },
      {
        name: $localize`Load Balancing Group`,
        prop: 'load_balancing_group',
        flexGrow: 1.5
      }
    ];
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
    this.nvmeofService
      .listNamespaces(this.subsystemNQN, this.group)
      .subscribe((res: NvmeofSubsystemNamespace[]) => {
        this.namespaces = res;
      });
  }

  deleteNamespaceModal() {
    const namespace = this.selection.first();
    this.modalService.show(CriticalConfirmationModalComponent, {
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
