import {
  Component,
  EventEmitter,
  Input,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
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
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { catchError, switchMap } from 'rxjs/operators';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { TopicConfiguration } from '~/app/shared/models/notification-configuration.model';
import { RgwNotificationFormComponent } from '../rgw-notification-form/rgw-notification-form.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Icons } from '~/app/shared/enum/icons.enum';

const BASE_URL = 'rgw/bucket';
@Component({
  selector: 'cd-rgw-bucket-notification-list',
  templateUrl: './rgw-bucket-notification-list.component.html',
  styleUrl: './rgw-bucket-notification-list.component.scss',
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwBucketNotificationListComponent extends ListWithDetails implements OnInit {
  @Input() bucket: Bucket;
  @Output() updateBucketDetails = new EventEmitter();
  @ViewChild(TableComponent, { static: true })
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
  modalRef: any;
  constructor(
    private rgwBucketService: RgwBucketService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private notificationService: NotificationService
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
        name: $localize`Destination`,
        prop: 'Destination',
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

  openNotificationModal(type: string) {
    const modalRef = this.modalService.show(RgwNotificationFormComponent, {
      bucket: this.bucket,
      selectedNotification: this.selection.first(),
      editing: type === this.actionLabels.EDIT ? true : false
    });
    modalRef?.close?.subscribe(() => this.updateBucketDetails.emit());
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const selectedNotificationId = this.selection.selected.map((notification) => notification.Id);
    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Notification`,
      itemNames: selectedNotificationId,
      actionDescription: $localize`delete`,
      submitAction: () => this.submitDeleteNotifications(selectedNotificationId)
    });
  }

  submitDeleteNotifications(notificationId: string[]) {
    this.rgwBucketService
      .deleteNotification(this.bucket.bucket, notificationId.join(','))
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Notifications deleted successfully.`
          );
          this.modalService.dismissAll();
        },
        error: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Failed to delete notifications. Please try again.`
          );
        }
      });
    this.modalRef?.close?.subscribe(() => this.updateBucketDetails.emit());
  }
}
