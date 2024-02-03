import {
  Component,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { BehaviorSubject, Observable, Subscription, of, timer } from 'rxjs';
import { finalize, map, shareReplay, switchMap } from 'rxjs/operators';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CephfsSnapshotscheduleFormComponent } from '../cephfs-snapshotschedule-form/cephfs-snapshotschedule-form.component';

@Component({
  selector: 'cd-cephfs-snapshotschedule-list',
  templateUrl: './cephfs-snapshotschedule-list.component.html',
  styleUrls: ['./cephfs-snapshotschedule-list.component.scss']
})
export class CephfsSnapshotscheduleListComponent
  extends CdForm
  implements OnInit, OnChanges, OnDestroy {
  @Input() fsName!: string;
  @Input() id!: number;

  @ViewChild('pathTpl', { static: true })
  pathTpl: any;

  @BlockUI()
  blockUI: NgBlockUI;

  snapshotSchedules$!: Observable<SnapshotSchedule[]>;
  subject$ = new BehaviorSubject<SnapshotSchedule[]>([]);
  snapScheduleModuleStatus$ = new BehaviorSubject<boolean>(false);
  moduleServiceListSub!: Subscription;
  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[] = [];
  context!: CdTableFetchDataContext;
  selection = new CdTableSelection();
  permissions!: Permissions;
  modalRef!: NgbModalRef;
  errorMessage: string = '';
  selectedName: string = '';
  icons = Icons;

  MODULE_NAME = 'snap_schedule';
  ENABLE_MODULE_TIMER = 2 * 1000;

  constructor(
    private snapshotScheduleService: CephfsSnapshotScheduleService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private actionLables: ActionLabelsI18n
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.fsName) {
      this.subject$.next([]);
    }
  }

  ngOnInit(): void {
    this.moduleServiceListSub = this.mgrModuleService
      .list()
      .pipe(
        map((modules: any[]) => modules.find((module) => module?.['name'] === this.MODULE_NAME))
      )
      .subscribe({
        next: (module: any) => this.snapScheduleModuleStatus$.next(module?.enabled)
      });

    this.snapshotSchedules$ = this.subject$.pipe(
      switchMap(() =>
        this.snapScheduleModuleStatus$.pipe(
          switchMap((status) => {
            if (!status) {
              return of([]);
            }
            return this.snapshotScheduleService.getSnapshotScheduleList('/', this.fsName);
          }),
          shareReplay(1)
        )
      )
    );

    this.columns = [
      { prop: 'path', name: $localize`Path`, flexGrow: 3, cellTemplate: this.pathTpl },
      { prop: 'subvol', name: $localize`Subvolume` },
      { prop: 'schedule', name: $localize`Repeat interval` },
      { prop: 'retention', name: $localize`Retention policy` },
      { prop: 'created_count', name: $localize`Created Count` },
      { prop: 'pruned_count', name: $localize`Deleted Count` },
      { prop: 'start', name: $localize`Start time`, cellTransformation: CellTemplate.timeAgo },
      { prop: 'created', name: $localize`Created`, cellTransformation: CellTemplate.timeAgo }
    ];

    this.tableActions = [
      {
        name: this.actionLables.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal(true)
      }
    ];
  }

  ngOnDestroy(): void {
    this.moduleServiceListSub.unsubscribe();
  }

  fetchData() {
    this.subject$.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSnapshotscheduleFormComponent,
      {
        fsName: this.fsName,
        id: this.id,
        path: this.selection?.first()?.path,
        isEdit: edit
      },
      { size: 'lg' }
    );
  }

  enableSnapshotSchedule() {
    let $obs;
    const fnWaitUntilReconnected = () => {
      timer(this.ENABLE_MODULE_TIMER).subscribe(() => {
        // Trigger an API request to check if the connection is
        // re-established.
        this.mgrModuleService.list().subscribe(
          () => {
            // Resume showing the notification toasties.
            this.notificationService.suspendToasties(false);
            // Unblock the whole UI.
            this.blockUI.stop();
            // Reload the data table content.
            this.notificationService.show(
              NotificationType.success,
              $localize`Enabled Snapshot Schedule Module`
            );
            // Reload the data table content.
          },
          () => {
            fnWaitUntilReconnected();
          }
        );
      });
    };

    if (!this.snapScheduleModuleStatus$.value) {
      $obs = this.mgrModuleService
        .enable(this.MODULE_NAME)
        .pipe(finalize(() => this.snapScheduleModuleStatus$.next(true)));
    }
    $obs.subscribe(
      () => undefined,
      () => {
        // Suspend showing the notification toasties.
        this.notificationService.suspendToasties(true);
        // Block the whole UI to prevent user interactions until
        // the connection to the backend is reestablished
        this.blockUI.start($localize`Reconnecting, please wait ...`);
        fnWaitUntilReconnected();
      }
    );
  }
}
