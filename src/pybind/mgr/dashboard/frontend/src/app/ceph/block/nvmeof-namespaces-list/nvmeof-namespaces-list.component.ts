import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
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
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;
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
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.namespacesColumns = [
      {
        name: $localize`Namespace ID`,
        prop: 'nsid'
      },
      {
        name: $localize`Size`,
        prop: 'rbd_image_size',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`Pool`,
        prop: 'rbd_pool_name'
      },
      {
        name: $localize`Image`,
        prop: 'rbd_image_name'
      },
      {
        name: $localize`Subsystem`,
        prop: 'ns_subsystem_nqn'
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
    this.nvmeofService.getNamespaceList().subscribe((res) => {
      this.namespaces = res as any;
    });
  }

  deleteNamespaceModal() {
    const namespace = this.selection.first();
    this.group = 'Test1';
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Namespace`,
      impact: DeletionImpact.high,
      bodyTemplate: this.deleteTpl,
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
