import { Component, ViewChild } from '@angular/core';

import { BsModalService } from 'ngx-bootstrap';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-user-list',
  templateUrl: './rgw-user-list.component.html',
  styleUrls: ['./rgw-user-list.component.scss']
})
export class RgwUserListComponent {
  @ViewChild(TableComponent)
  table: TableComponent;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  users: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private rgwUserService: RgwUserService,
    private bsModalService: BsModalService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: 'Username',
        prop: 'user_id',
        flexGrow: 1
      },
      {
        name: 'Full name',
        prop: 'display_name',
        flexGrow: 1
      },
      {
        name: 'Email address',
        prop: 'email',
        flexGrow: 1
      },
      {
        name: 'Suspended',
        prop: 'suspended',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: 'Max. buckets',
        prop: 'max_buckets',
        flexGrow: 1
      }
    ];
    const getUserUri = () => this.selection.first() && this.selection.first().user_id;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => '/rgw/user/add',
      name: 'Add'
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => `/rgw/user/edit/${getUserUri()}`,
      name: 'Edit'
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-trash-o',
      click: () => this.deleteAction(),
      name: 'Delete'
    };
    this.tableActions = [addAction, editAction, deleteAction];
  }

  getUserList(context: CdTableFetchDataContext) {
    this.rgwUserService.list().subscribe(
      (resp: object[]) => {
        this.users = resp;
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    this.bsModalService.show(DeletionModalComponent, {
      initialState: {
        itemDescription: this.selection.hasSingleSelection ? 'user' : 'users',
        submitActionObservable: (): Observable<any> => {
          return new Observable((observer: Subscriber<any>) => {
            // Delete all selected data table rows.
            observableForkJoin(
              this.selection.selected.map((user: any) => {
                return this.rgwUserService.delete(user.user_id);
              })
            ).subscribe(
              null,
              (error) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            );
          });
        }
      }
    });
  }
}
