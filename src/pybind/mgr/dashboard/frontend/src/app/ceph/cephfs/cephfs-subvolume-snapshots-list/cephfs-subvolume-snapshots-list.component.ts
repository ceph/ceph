import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { BehaviorSubject, Observable, forkJoin, of } from 'rxjs';
import { catchError, shareReplay, switchMap, tap } from 'rxjs/operators';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CephfsSubvolume, SubvolumeSnapshot } from '~/app/shared/models/cephfs-subvolume.model';

@Component({
  selector: 'cd-cephfs-subvolume-snapshots-list',
  templateUrl: './cephfs-subvolume-snapshots-list.component.html',
  styleUrls: ['./cephfs-subvolume-snapshots-list.component.scss']
})
export class CephfsSubvolumeSnapshotsListComponent implements OnInit, OnChanges {
  @Input() fsName: string;

  context: CdTableFetchDataContext;
  columns: CdTableColumn[] = [];

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
    private cephfsSubvolumeService: CephfsSubvolumeService
  ) {}

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
        cellTransformation: CellTemplate.timeAgo
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
}
