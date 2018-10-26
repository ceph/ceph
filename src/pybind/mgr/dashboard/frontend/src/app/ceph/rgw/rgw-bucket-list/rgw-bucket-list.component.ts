import { Component, ViewChild } from '@angular/core';

import { BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-bucket-list',
  templateUrl: './rgw-bucket-list.component.html',
  styleUrls: ['./rgw-bucket-list.component.scss']
})
export class RgwBucketListComponent {
  @ViewChild(TableComponent)
  table: TableComponent;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  buckets: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private rgwBucketService: RgwBucketService,
    private bsModalService: BsModalService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: 'Name',
        prop: 'bucket',
        flexGrow: 1
      },
      {
        name: 'Owner',
        prop: 'owner',
        flexGrow: 1
      }
    ];
    const getBucketUri = () =>
      this.selection.first() && `${encodeURI(this.selection.first().bucket)}`;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => '/rgw/bucket/add',
      name: 'Add'
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => `/rgw/bucket/edit/${getBucketUri()}`,
      name: 'Edit'
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-times',
      click: () => this.deleteAction(),
      name: 'Delete'
    };
    this.tableActions = [addAction, editAction, deleteAction];
  }

  getBucketList(context: CdTableFetchDataContext) {
    this.rgwBucketService.list().subscribe(
      (resp: object[]) => {
        this.buckets = resp;
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
    this.bsModalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: this.selection.hasSingleSelection ? 'bucket' : 'buckets',
        submitActionObservable: () => {
          return new Observable((observer: Subscriber<any>) => {
            // Delete all selected data table rows.
            observableForkJoin(
              this.selection.selected.map((bucket: any) => {
                return this.rgwBucketService.delete(bucket.bucket);
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
