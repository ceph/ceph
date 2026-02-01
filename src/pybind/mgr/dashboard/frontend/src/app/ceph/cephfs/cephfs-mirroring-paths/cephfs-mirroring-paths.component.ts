import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';

@Component({
  selector: 'cd-cephfs-mirroring-paths',
  templateUrl: './cephfs-mirroring-paths.component.html',
  styleUrls: ['./cephfs-mirroring-paths.component.scss'],
  standalone: false
})
export class CephfsMirroringPathsComponent implements OnInit {
  remoteFsName: string;
  localFsName: string;
  remoteClusterName: string;

  columns: CdTableColumn[];
  mirroringPaths$: Observable<any[]>;

  private navigationState: Record<string, string> | null = null;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private cephfsService: CephfsService
  ) {
    const nav = this.router.getCurrentNavigation();
    if (nav?.extras?.state) {
      this.navigationState = nav.extras.state as Record<string, string>;
    }
  }

  ngOnInit() {
    const state = this.navigationState;
    this.localFsName = this.route.snapshot.params['fsName'] || state?.localFsName || '-';
    this.remoteFsName = state?.remoteFsName ?? '-';
    this.remoteClusterName = state?.remoteClusterName ?? '-';

    this.columns = [
      { name: $localize`Path`, prop: 'path', flexGrow: 2 },
      { name: $localize`Type`, prop: 'type', flexGrow: 1 },
      { name: $localize`Status`, prop: 'status', flexGrow: 1 },
      { name: $localize`Snapshot schedule`, prop: 'snapshot_schedule', flexGrow: 1 },
      { name: $localize`Last sync`, prop: 'last_sync', flexGrow: 1 }
    ];

    // TODO: Update data displayed when new interface is implemented.
    this.mirroringPaths$ = this.cephfsService.listSnapshotDirs(this.localFsName).pipe(
      map((paths) =>
        paths.map((path) => ({
          path,
          type: '',
          status: '',
          snapshot_schedule: '',
          last_sync: ''
        }))
      ),
      catchError(() => of([]))
    );
  }
}
