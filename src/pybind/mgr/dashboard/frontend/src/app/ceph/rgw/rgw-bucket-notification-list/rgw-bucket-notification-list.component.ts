import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Bucket } from '../models/rgw-bucket';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { catchError, switchMap } from 'rxjs/operators';
import { TopicConfiguration } from '~/app/shared/models/notification-configuration.model';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

const BASE_URL = 'rgw/bucket';
@Component({
  selector: 'cd-rgw-bucket-notification-list',
  templateUrl: './rgw-bucket-notification-list.component.html',
  styleUrl: './rgw-bucket-notification-list.component.scss',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketNotificationListComponent extends ListWithDetails implements OnInit {
  @Input() bucket: Bucket;
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  notification$: Observable<TopicConfiguration[]>;
  subject = new BehaviorSubject<TopicConfiguration[]>([]);
  context: CdTableFetchDataContext;
  @ViewChild('filterTpl', { static: true })
  filterTpl: TemplateRef<any>;
  @ViewChild('eventTpl', { static: true })
  eventTpl: TemplateRef<any>;

  constructor(
    private rgwBucketService: RgwBucketService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
  }

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'Id',
        flexGrow: 2
      },
      {
        name: $localize`Topic`,
        prop: 'Topic',
        flexGrow: 1,
        cellTransformation: CellTemplate.copy
      },
      {
        name: $localize`Event`,
        prop: 'Event',
        flexGrow: 1,
        cellTemplate: this.eventTpl
      },
      {
        name: $localize`Filter`,
        prop: 'Filter',
        flexGrow: 1,
        cellTemplate: this.filterTpl
      }
    ];

    this.notification$ = this.subject.pipe(
      switchMap(() =>
        this.rgwBucketService.listNotification(this.bucket.bucket).pipe(
          catchError((error) => {
            this.context.error(error);
            return of(null);
          })
        )
      )
    );
  }

  fetchData() {
    this.subject.next([]);
  }
}
