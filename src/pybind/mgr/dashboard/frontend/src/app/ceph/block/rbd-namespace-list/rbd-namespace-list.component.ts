import { Component, OnInit } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin, Observable } from 'rxjs';

import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { RbdNamespaceFormModalComponent } from '../rbd-namespace-form/rbd-namespace-form-modal.component';

@Component({
  selector: 'cd-rbd-namespace-list',
  templateUrl: './rbd-namespace-list.component.html',
  styleUrls: ['./rbd-namespace-list.component.scss'],
  providers: [TaskListService]
})
export class RbdNamespaceListComponent implements OnInit {
  columns: CdTableColumn[];
  namespaces: any;
  modalRef: NgbModalRef;
  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    private poolService: PoolService,
    private modalService: ModalService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      click: () => this.createModal(),
      name: this.actionLabels.CREATE
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteModal(),
      name: this.actionLabels.DELETE,
      disable: () => this.getDeleteDisableDesc()
    };
    this.tableActions = [createAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Namespace`,
        prop: 'namespace',
        flexGrow: 1
      },
      {
        name: $localize`Pool`,
        prop: 'pool',
        flexGrow: 1
      },
      {
        name: $localize`Total images`,
        prop: 'num_images',
        flexGrow: 1
      }
    ];
    this.refresh();
  }

  refresh() {
    this.poolService.list(['pool_name', 'type', 'application_metadata']).then((pools: any) => {
      pools = pools.filter(
        (pool: any) => this.rbdService.isRBDPool(pool) && pool.type === 'replicated'
      );
      const promises: Observable<any>[] = [];
      pools.forEach((pool: any) => {
        promises.push(this.rbdService.listNamespaces(pool['pool_name']));
      });
      if (promises.length > 0) {
        forkJoin(promises).subscribe((data: Array<Array<string>>) => {
          const result: any[] = [];
          for (let i = 0; i < data.length; i++) {
            const namespaces = data[i];
            const pool_name = pools[i]['pool_name'];
            namespaces.forEach((namespace: any) => {
              result.push({
                id: `${pool_name}/${namespace.namespace}`,
                pool: pool_name,
                namespace: namespace.namespace,
                num_images: namespace.num_images
              });
            });
          }
          this.namespaces = result;
        });
      } else {
        this.namespaces = [];
      }
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  createModal() {
    this.modalRef = this.modalService.show(RbdNamespaceFormModalComponent);
    this.modalRef.componentInstance.onSubmit.subscribe(() => {
      this.refresh();
    });
  }

  deleteModal() {
    const pool = this.selection.first().pool;
    const namespace = this.selection.first().namespace;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Namespace',
      itemNames: [`${pool}/${namespace}`],
      submitAction: () =>
        this.rbdService.deleteNamespace(pool, namespace).subscribe(
          () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Deleted namespace '${pool}/${namespace}'`
            );
            this.modalRef.close();
            this.refresh();
          },
          () => {
            this.modalRef.componentInstance.stopLoadingSpinner();
          }
        )
    });
  }

  getDeleteDisableDesc(): string | boolean {
    const first = this.selection.first();

    if (first?.num_images > 0) {
      return $localize`Namespace contains images`;
    }

    return !this.selection?.first();
  }
}
