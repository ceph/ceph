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
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CephfsSnapshotscheduleFormComponent } from '../cephfs-snapshotschedule-form/cephfs-snapshotschedule-form.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';

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

  @ViewChild('retentionTpl', { static: true })
  retentionTpl: any;

  @ViewChild('subvolTpl', { static: true })
  subvolTpl: any;

  @BlockUI()
  blockUI: NgBlockUI;

  snapshotSchedules$!: Observable<SnapshotSchedule[]>;
  subject$ = new BehaviorSubject<SnapshotSchedule[]>([]);
  snapScheduleModuleStatus$ = new BehaviorSubject<boolean>(false);
  moduleServiceListSub!: Subscription;
  columns: CdTableColumn[] = [];
  tableActions$ = new BehaviorSubject<CdTableAction[]>([]);
  context!: CdTableFetchDataContext;
  selection = new CdTableSelection();
  permissions!: Permissions;
  modalRef!: NgbModalRef;
  errorMessage: string = '';
  selectedName: string = '';
  icons = Icons;
  tableActions: CdTableAction[] = [
    {
      name: this.actionLabels.CREATE,
      permission: 'create',
      icon: Icons.add,
      click: () => this.openModal(false)
    },
    {
      name: this.actionLabels.EDIT,
      permission: 'update',
      icon: Icons.edit,
      click: () => this.openModal(true)
    },
    {
      name: this.actionLabels.DELETE,
      permission: 'delete',
      icon: Icons.trash,
      click: () => this.deleteSnapshotSchedule()
    }
  ];

  MODULE_NAME = 'snap_schedule';
  ENABLE_MODULE_TIMER = 2 * 1000;

  constructor(
    private snapshotScheduleService: CephfsSnapshotScheduleService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private actionLabels: ActionLabelsI18n,
    private taskWrapper: TaskWrapperService
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
            return this.snapshotScheduleService
              .getSnapshotScheduleList('/', this.fsName)
              .pipe(
                map((list) =>
                  list.map((l) => ({ ...l, pathForSelection: `${l.path}@${l.schedule}` }))
                )
              );
          }),
          shareReplay(1)
        )
      )
    );

    this.columns = [
      { prop: 'pathForSelection', name: $localize`Path`, flexGrow: 3, cellTemplate: this.pathTpl },
      { prop: 'path', isHidden: true, isInvisible: true },
      { prop: 'subvol', name: $localize`Subvolume`, cellTemplate: this.subvolTpl },
      { prop: 'scheduleCopy', name: $localize`Repeat interval` },
      { prop: 'schedule', isHidden: true, isInvisible: true },
      { prop: 'retentionCopy', name: $localize`Retention policy`, cellTemplate: this.retentionTpl },
      { prop: 'retention', isHidden: true, isInvisible: true },
      { prop: 'created_count', name: $localize`Created Count` },
      { prop: 'pruned_count', name: $localize`Deleted Count` },
      { prop: 'start', name: $localize`Start time`, cellTransformation: CellTemplate.timeAgo },
      { prop: 'created', name: $localize`Created`, cellTransformation: CellTemplate.timeAgo }
    ];

    this.tableActions$.next(this.tableActions);
  }

  ngOnDestroy(): void {
    this.moduleServiceListSub.unsubscribe();
  }

  fetchData() {
    this.subject$.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    if (!this.selection.hasSelection) return;
    const isActive = this.selection.first()?.active;

    this.tableActions$.next([
      ...this.tableActions,
      {
        name: isActive ? this.actionLabels.DEACTIVATE : this.actionLabels.ACTIVATE,
        permission: 'update',
        icon: isActive ? Icons.warning : Icons.success,
        click: () =>
          isActive ? this.deactivateSnapshotSchedule() : this.activateSnapshotSchedule()
      }
    ]);
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSnapshotscheduleFormComponent,
      {
        fsName: this.fsName,
        id: this.id,
        path: this.selection?.first()?.path,
        schedule: this.selection?.first()?.schedule,
        retention: this.selection?.first()?.retention,
        start: this.selection?.first()?.start,
        status: this.selection?.first()?.status,
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

  deactivateSnapshotSchedule() {
    const { path, start, fs, schedule, subvol, group } = this.selection.first();

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`snapshot schedule`,
      actionDescription: this.actionLabels.DEACTIVATE,
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/snapshot/schedule/deactivate', {
            path
          }),
          call: this.snapshotScheduleService.deactivate({
            path,
            schedule,
            start,
            fs,
            subvol,
            group
          })
        })
    });
  }

  activateSnapshotSchedule() {
    const { path, start, fs, schedule, subvol, group } = this.selection.first();

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`snapshot schedule`,
      actionDescription: this.actionLabels.ACTIVATE,
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/snapshot/schedule/activate', {
            path
          }),
          call: this.snapshotScheduleService.activate({
            path,
            schedule,
            start,
            fs,
            subvol,
            group
          })
        })
    });
  }

  deleteSnapshotSchedule() {
    const { path, start, fs, schedule, subvol, group, retention } = this.selection.first();
    const retentionPolicy = retention
      ?.split(/\s/gi)
      ?.filter((r: string) => !!r)
      ?.map((r: string) => {
        const frequency = r.substring(r.length - 1);
        const interval = r.substring(0, r.length - 1);
        return `${interval}-${frequency}`;
      })
      ?.join('|')
      ?.toLocaleLowerCase();

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`snapshot schedule`,
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.DELETE, {
            path
          }),
          call: this.snapshotScheduleService.delete({
            path,
            schedule,
            start,
            fs,
            retentionPolicy,
            subvol,
            group
          })
        })
    });
  }
}
