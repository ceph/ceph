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
    private cdDatePipe: CdDatePipe
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
}
