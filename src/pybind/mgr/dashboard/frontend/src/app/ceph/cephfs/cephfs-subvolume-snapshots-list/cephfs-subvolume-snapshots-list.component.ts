import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { BehaviorSubject, Observable, forkJoin, of } from 'rxjs';
import { catchError, shareReplay, switchMap, tap } from 'rxjs/operators';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CephfsSubvolume, SubvolumeSnapshot } from '~/app/shared/models/cephfs-subvolume.model';
import { CephfsSubvolumeSnapshotsFormComponent } from './cephfs-subvolume-snapshots-form/cephfs-subvolume-snapshots-form.component';
import { ModalService } from '~/app/shared/services/modal.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import moment from 'moment';
import { Validators } from '@angular/forms';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-cephfs-subvolume-snapshots-list',
  templateUrl: './cephfs-subvolume-snapshots-list.component.html',
  styleUrls: ['./cephfs-subvolume-snapshots-list.component.scss']
})
export class CephfsSubvolumeSnapshotsListComponent implements OnInit, OnChanges {
  @Input() fsName: string;

  context: CdTableFetchDataContext;
  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permissions: Permissions;
  modalRef: NgbModalRef;

  subVolumes$: Observable<CephfsSubvolume[]>;
  snapshots$: Observable<any[]>;
  snapshotSubject = new BehaviorSubject<SubvolumeSnapshot[]>([]);
  subVolumeSubject = new BehaviorSubject<CephfsSubvolume[]>([]);

  subvolumeGroupList: string[] = [];
  subVolumesList: string[];

  activeGroupName = '';
  activeSubVolumeName = '';

  isSubVolumesAvailable = false;
  isLoading = true;

  observables: any = [];

  constructor(
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private cephfsSubvolumeService: CephfsSubvolumeService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private authStorageService: AuthStorageService,
    private cdDatePipe: CdDatePipe,
    private taskWrapper: TaskWrapperService,
    private notificationService: NotificationService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Created`,
        prop: 'info.created_at',
        flexGrow: 1,
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Pending Clones`,
        prop: 'info.has_pending_clones',
        flexGrow: 0.5,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            no: { class: 'badge-success' },
            yes: { class: 'badge-info' }
          }
        }
      }
    ];

    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal()
      },
      {
        name: this.actionLabels.CLONE,
        permission: 'create',
        icon: Icons.clone,
        disable: () => !this.selection.hasSingleSelection,
        click: () => this.cloneModal()
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        disable: () => !this.selection.hasSingleSelection,
        click: () => this.deleteSnapshot()
      }
    ];

    this.cephfsSubvolumeGroupService
      .get(this.fsName)
      .pipe(
        switchMap((groups) => {
          // manually adding the group '_nogroup' to the list.
          groups.unshift({ name: '' });

          const observables = groups.map((group) =>
            this.cephfsSubvolumeService.existsInFs(this.fsName, group.name).pipe(
              switchMap((resp) => {
                if (resp) {
                  this.subvolumeGroupList.push(group.name);
                }
                return of(resp); // Emit the response
              })
            )
          );

          return forkJoin(observables);
        })
      )
      .subscribe(() => {
        if (this.subvolumeGroupList.length) {
          this.isSubVolumesAvailable = true;
        }
        this.isLoading = false;
      });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.fsName) {
      this.subVolumeSubject.next([]);
    }
  }

  selectSubVolumeGroup(subVolumeGroupName: string) {
    this.activeGroupName = subVolumeGroupName;
    this.getSubVolumes();
  }

  selectSubVolume(subVolumeName: string) {
    this.activeSubVolumeName = subVolumeName;
    this.getSubVolumesSnapshot();
  }

  getSubVolumes() {
    this.subVolumes$ = this.subVolumeSubject.pipe(
      switchMap(() =>
        this.cephfsSubvolumeService.get(this.fsName, this.activeGroupName, false).pipe(
          tap((resp) => {
            this.subVolumesList = resp.map((subVolume) => subVolume.name);
            this.activeSubVolumeName = resp[0].name;
            this.getSubVolumesSnapshot();
          })
        )
      )
    );
  }

  getSubVolumesSnapshot() {
    this.snapshots$ = this.snapshotSubject.pipe(
      switchMap(() =>
        this.cephfsSubvolumeService
          .getSnapshots(this.fsName, this.activeSubVolumeName, this.activeGroupName)
          .pipe(
            catchError(() => {
              this.context.error();
              return of(null);
            })
          )
      ),
      shareReplay(1)
    );
  }

  fetchData() {
    this.snapshotSubject.next([]);
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSubvolumeSnapshotsFormComponent,
      {
        fsName: this.fsName,
        subVolumeName: this.activeSubVolumeName,
        subVolumeGroupName: this.activeGroupName,
        subVolumeGroups: this.subvolumeGroupList,
        isEdit: edit
      },
      { size: 'lg' }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteSnapshot() {
    const snapshotName = this.selection.first().name;
    const subVolumeName = this.activeSubVolumeName;
    const subVolumeGroupName = this.activeGroupName;
    const fsName = this.fsName;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      actionDescription: 'Delete',
      itemNames: [snapshotName],
      itemDescription: 'Snapshot',
      submitAction: () =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('cephfs/subvolume/snapshot/delete', {
              fsName: fsName,
              subVolumeName: subVolumeName,
              subVolumeGroupName: subVolumeGroupName,
              snapshotName: snapshotName
            }),
            call: this.cephfsSubvolumeService.deleteSnapshot(
              fsName,
              subVolumeName,
              snapshotName,
              subVolumeGroupName
            )
          })
          .subscribe({
            complete: () => this.modalRef.close(),
            error: () => this.modalRef.componentInstance.stopLoadingSpinner()
          })
    });
  }

  cloneModal() {
    const cloneName = `clone_${moment().toISOString(true)}`;
    const allGroups = Array.from(this.subvolumeGroupList).map((group) => {
      return { value: group, text: group === '' ? '_nogroup' : group };
    });
    this.modalService.show(FormModalComponent, {
      titleText: $localize`Create clone`,
      fields: [
        {
          type: 'text',
          name: 'cloneName',
          value: cloneName,
          label: $localize`Name`,
          validators: [Validators.required, Validators.pattern(/^[.A-Za-z0-9_+:-]+$/)],
          asyncValidators: [
            CdValidators.unique(
              this.cephfsSubvolumeService.exists,
              this.cephfsSubvolumeService,
              null,
              null,
              this.fsName
            )
          ],
          required: true,
          errors: {
            pattern: $localize`Allowed characters are letters, numbers, '.', '-', '+', ':' or '_'`,
            notUnique: $localize`A subvolume or clone with this name already exists.`
          }
        },
        {
          type: 'select',
          name: 'groupName',
          value: this.activeGroupName,
          label: $localize`Group name`,
          typeConfig: {
            options: allGroups
          }
        }
      ],
      submitButtonText: $localize`Create Clone`,
      onSubmit: (value: any) => {
        this.cephfsSubvolumeService
          .createSnapshotClone(
            this.fsName,
            this.activeSubVolumeName,
            this.selection.first().name,
            value.cloneName,
            this.activeGroupName,
            value.groupName
          )
          .subscribe(() =>
            this.notificationService.show(
              NotificationType.success,
              $localize`Created Clone "${value.cloneName}" successfully.`
            )
          );
      }
    });
  }
}
