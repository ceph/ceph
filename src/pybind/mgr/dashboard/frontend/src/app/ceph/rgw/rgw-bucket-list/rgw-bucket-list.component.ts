import { Component, NgZone, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, Observable, Subscriber } from 'rxjs';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

const BASE_URL = 'rgw/bucket';

@Component({
  selector: 'cd-rgw-bucket-list',
  templateUrl: './rgw-bucket-list.component.html',
  styleUrls: ['./rgw-bucket-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketListComponent extends ListWithDetails {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  buckets: object[] = [];
  selection: CdTableSelection = new CdTableSelection();
  isStale = false;
  staleTimeout: number;

  constructor(
    private authStorageService: AuthStorageService,
    private rgwBucketService: RgwBucketService,
    private bsModalService: BsModalService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n,
    private ngZone: NgZone
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'bid',
        flexGrow: 1
      },
      {
        name: this.i18n('Owner'),
        prop: 'owner',
        flexGrow: 1
      }
    ];
    const getBucketUri = () =>
      this.selection.first() && `${encodeURIComponent(this.selection.first().bid)}`;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => this.urlBuilder.getEdit(getBucketUri()),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteAction(),
      disable: () => !this.selection.hasSelection,
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.tableActions = [addAction, editAction, deleteAction];
    this.timeConditionReached();
  }

  timeConditionReached() {
    clearTimeout(this.staleTimeout);
    this.ngZone.runOutsideAngular(() => {
      this.staleTimeout = window.setTimeout(() => {
        this.ngZone.run(() => {
          this.isStale = true;
        });
      }, 10000);
    });
  }

  getBucketList(context: CdTableFetchDataContext) {
    this.isStale = false;
    this.timeConditionReached();
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
        itemDescription: this.selection.hasSingleSelection
          ? this.i18n('bucket')
          : this.i18n('buckets'),
        itemNames: this.selection.selected.map((bucket: any) => bucket['bid']),
        submitActionObservable: () => {
          return new Observable((observer: Subscriber<any>) => {
            // Delete all selected data table rows.
            observableForkJoin(
              this.selection.selected.map((bucket: any) => {
                return this.rgwBucketService.delete(bucket.bid);
              })
            ).subscribe({
              error: (error) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              complete: () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            });
          });
        }
      }
    });
  }
}
