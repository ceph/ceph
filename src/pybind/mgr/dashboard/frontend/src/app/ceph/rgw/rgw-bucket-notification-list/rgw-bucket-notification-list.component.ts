import { Component, EventEmitter, Input, OnInit, Output, TemplateRef, ViewChild } from '@angular/core';
import { Observable, of, Subscriber } from 'rxjs';
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
import { catchError, tap } from 'rxjs/operators';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwCreateNotificationFormComponent } from '../rgw-create-notification-form/rgw-create-notification-form.component';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { TopicConfiguration } from '~/app/shared/models/notification-configuration.model';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';


const BASE_URL = 'rgw/bucket';
@Component({
  selector: 'cd-rgw-bucket-notification-list',
  templateUrl: './rgw-bucket-notification-list.component.html',
  styleUrl: './rgw-bucket-notification-list.component.scss',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketNotificationListComponent extends ListWithDetails implements OnInit {
  @Input() bucket: Bucket;
  @Output() updateBucketDetails = new EventEmitter(); // Define output event
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  errorMessage: string;
  selection: CdTableSelection = new CdTableSelection();
  notification$: Observable<TopicConfiguration[]>;
  notificationList: TopicConfiguration;
  modalRef: any;
  @ViewChild('filterTpl', { static: true })
  filterTpl: TemplateRef<any>;

  constructor(
    private rgwBucketService: RgwBucketService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
   
    this.columns = [
      {
        name: $localize`Name`,
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
        flexGrow: 1,
        cellTemplate: this.filterTpl
      }
    ];
    const createAction: CdTableAction = {
          permission: 'create',
          icon: Icons.add,
          click: () => this.openNotificationModal(this.actionLabels.CREATE),
          name: this.actionLabels.CREATE
        };
        const editAction: CdTableAction = {
          permission: 'update',
          icon: Icons.edit,
          disable: () => this.selection.hasMultiSelection,
          click: () => this.openNotificationModal(this.actionLabels.EDIT),
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
        this.tableActions = [createAction, editAction, deleteAction];
  }

  loadNotification(context: CdTableFetchDataContext) {
    this.notification$ = this.rgwBucketService.getBucketNotificationList(this.bucket.bucket).pipe(
      tap((notification: TopicConfiguration) => {
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

  openNotificationModal(type: string) {
      const modalRef = this.modalService.show(RgwCreateNotificationFormComponent, {
        bucket: this.bucket,
        selectedNotification: this.selection.first(),
        editing: type === this.actionLabels.EDIT ? true : false
      });
      modalRef?.close?.subscribe(() => this.updateBucketDetails.emit());
    }

    deleteAction() {
      const notificationId = this.selection.first().Id;
      // Show a confirmation modal with details
      this.modalService.show(DeleteConfirmationModalComponent, {
        itemDescription: $localize`Notification`,  
        itemNames: [notificationId],             
        submitActionObservable: () => {    
          return new Observable((observer: Subscriber<any>) => {
            this.taskWrapper
              .wrapTaskAroundCall({
                task: new FinishedTask('rgw/bucket/notification/delete', {
                  Id: [notificationId]  // Pass the notification ID to the task
                }),
                call: this.rgwBucketService.deleteNotification(this.bucket.bucket,notificationId)  // Call to delete the notification
              })
              .subscribe({
                error: (error: any) => {
                  observer.error(error); 
                },
                complete: () => {
                  observer.complete();   
                  this.modalService.dismissAll(); 
                }
              });
          });
        }
      });
    }
    
    
}
