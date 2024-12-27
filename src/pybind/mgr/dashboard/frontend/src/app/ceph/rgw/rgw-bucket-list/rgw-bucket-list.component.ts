import { Component, NgZone, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import _ from 'lodash';
import { forkJoin as observableForkJoin, Observable, Subscriber, Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Bucket } from '../models/rgw-bucket';

const BASE_URL = 'rgw/bucket';

@Component({
  selector: 'cd-rgw-bucket-list',
  templateUrl: './rgw-bucket-list.component.html',
  styleUrls: ['./rgw-bucket-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketListComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @ViewChild('bucketSizeTpl', { static: true })
  bucketSizeTpl: TemplateRef<any>;
  @ViewChild('bucketObjectTpl', { static: true })
  bucketObjectTpl: TemplateRef<any>;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  buckets: Bucket[] = [];
  selection: CdTableSelection = new CdTableSelection();
  declare staleTimeout: number;
  private subs: Subscription = new Subscription();

  constructor(
    private authStorageService: AuthStorageService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private dimlessPipe: DimlessPipe,
    private rgwBucketService: RgwBucketService,
    private modalService: ModalCdsService,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n,
    protected ngZone: NgZone,
    private taskWrapper: TaskWrapperService
  ) {
    super(ngZone);
  }

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'bid',
        flexGrow: 2
      },
      {
        name: $localize`Owner`,
        prop: 'owner',
        flexGrow: 2.5
      },
      {
        name: $localize`Used Capacity`,
        prop: 'bucket_size',
        flexGrow: 0.6,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`Capacity Limit %`,
        prop: 'size_usage',
        cellTemplate: this.bucketSizeTpl,
        flexGrow: 0.8
      },
      {
        name: $localize`Objects`,
        prop: 'num_objects',
        flexGrow: 0.6,
        pipe: this.dimlessPipe
      },
      {
        name: $localize`Object Limit %`,
        prop: 'object_usage',
        cellTemplate: this.bucketObjectTpl,
        flexGrow: 0.8
      },
      {
        name: $localize`Number of Shards`,
        prop: 'num_shards',
        flexGrow: 0.8
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
    this.setTableRefreshTimeout();
  }

  getBucketList(context: CdTableFetchDataContext) {
    this.setTableRefreshTimeout();
    this.subs.add(
      this.rgwBucketService
        .fetchAndTransformBuckets()
        .pipe(switchMap(() => this.rgwBucketService.buckets$))
        .subscribe({
          next: (buckets) => {
            this.buckets = buckets;
          },
          error: () => context.error()
        })
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const itemNames = this.selection.selected.map((bucket: any) => bucket['bid']);
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: this.selection.hasSingleSelection ? $localize`bucket` : $localize`buckets`,
      itemNames: itemNames,
      bodyTemplate: this.deleteTpl,
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('rgw/bucket/delete', {
                bucket_names: itemNames
              }),
              call: observableForkJoin(
                this.selection.selected.map((bucket: any) => {
                  return this.rgwBucketService.delete(bucket.bid);
                })
              )
            })
            .subscribe({
              error: (error: any) => {
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
    });
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }
}
