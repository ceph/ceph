import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { Observable, of } from 'rxjs';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Bucket, Notification } from '../models/rgw-bucket';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { catchError, tap } from 'rxjs/operators';

const BASE_URL = 'rgw/bucket';
@Component({
  selector: 'cd-rgw-bucket-notification-list',
  templateUrl: './rgw-bucket-notification-list.component.html',
  styleUrl: './rgw-bucket-notification-list.component.scss',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketNotificationListComponent implements OnInit {
  @Input() bucket: Bucket;
  @Output() updateBucketDetails = new EventEmitter(); // Define output event
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  notification$: Observable<Notification[]>;
  notificationList: Notification;
  modalRef: any;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService
  ) {}

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Id`,
        prop: 'Id',
        flexGrow: 2
      },
      {
        name: $localize`Topic`,
        prop: 'Topic',
        flexGrow: 1
      },
      {
        name: $localize`Event`,
        prop: 'Event',
        flexGrow: 1
      },
      {
        name: $localize`Filter`,
        prop: 'Filter',
        flexGrow: 1
      }
    ];
  }

  loadNotification(context: CdTableFetchDataContext) {
    this.notification$ = this.rgwBucketService.getBucketNotificationList(this.bucket.bucket).pipe(
      tap((notification: any) => {
        this.notificationList = notification;
      }),
      catchError(() => {
        context.error();
        return of(null);
      })
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
