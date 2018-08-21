import { Component, ViewChild } from '@angular/core';

import { BsModalService } from 'ngx-bootstrap';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
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
    const modalRef = this.bsModalService.show(DeletionModalComponent);
    modalRef.content.setUp({
      metaType: this.selection.hasSingleSelection ? 'bucket' : 'buckets',
      deletionObserver: (): Observable<any> => {
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
      },
      modalRef: modalRef
    });
  }
}
