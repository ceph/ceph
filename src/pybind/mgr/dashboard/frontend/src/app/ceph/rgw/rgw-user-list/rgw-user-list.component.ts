import { Component, ViewChild } from '@angular/core';

import { BsModalService } from 'ngx-bootstrap';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
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
  @ViewChild(TableComponent) table: TableComponent;

  permission: Permission;
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
    const modalRef = this.bsModalService.show(DeletionModalComponent);
    modalRef.content.setUp({
      metaType: this.selection.hasSingleSelection ? 'user' : 'users',
      deletionObserver: (): Observable<any> => {
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
      },
      modalRef: modalRef
    });
  }
}
