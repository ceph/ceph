import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
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
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { combineLatest, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'cd-nvmeof-subsystem-namespaces-list',
  templateUrl: './nvmeof-subsystem-namespaces-list.component.html',
  styleUrls: ['./nvmeof-subsystem-namespaces-list.component.scss'],
  standalone: false
})
export class NvmeofSubsystemNamespacesListComponent implements OnInit, OnDestroy {
  subsystemNQN: string;
  group: string;
  namespacesColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  namespaces: NvmeofSubsystemNamespace[] = [];

  private destroy$ = new Subject<void>();

  constructor(
    // ... constructor stays mostly same
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
    combineLatest([this.route.parent?.params, this.route.queryParams])
      .pipe(takeUntil(this.destroy$))
      .subscribe(([params, qp]) => {
        this.subsystemNQN = params['subsystem_nqn'];
        this.group = qp['group'];
        if (this.subsystemNQN && this.group) {
          this.listNamespaces();
        }
      });

    this.setupColumns();
    this.setupTableActions();
  }

  setupColumns() {
    this.namespacesColumns = [
      {
        name: $localize`Namespace ID`,
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
      }
    ];
  }

  setupTableActions() {
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate(['block/nvmeof/namespaces/create'], {
            queryParams: {
              group: this.group,
              subsystem_nqn: this.subsystemNQN
            }
          }),

        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: $localize`Expand`,
        permission: 'update',
        icon: Icons.edit,
        click: (row: NvmeofSubsystemNamespace) => {
          const namespace = row || this.selection.first();
          this.router.navigate(
            [
              {
                outlets: {
                  modal: [URLVerbs.EDIT, this.subsystemNQN, 'namespace', namespace.nsid]
                }
              }
            ],
            {
              relativeTo: this.route.parent,
              queryParams: { group: this.group },
              queryParamsHandling: 'merge'
            }
          );
        }
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
        .listNamespaces(this.group, this.subsystemNQN)
        .pipe(takeUntil(this.destroy$))
        .subscribe((res: NvmeofSubsystemNamespace[]) => {
          this.namespaces = res || [];
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

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
