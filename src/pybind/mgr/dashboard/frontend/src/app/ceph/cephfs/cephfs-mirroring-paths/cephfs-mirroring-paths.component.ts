import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

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
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permissions: Permissions;
  icons = Icons;
  addPathPanelExpanded = false;
  addPathPanelTitle = $localize`Add mirroring path`;
  addPathForm = new FormGroup({
    selectedSubvolume: new FormControl(''),
    snapshotScheduleOption: new FormControl('existing')
  });
  subvolumeOptions: string[] = [];
  selectSubvolumeLabel = $localize`Select subvolume`;

  constructor(
    private route: ActivatedRoute,
    private cephfsService: CephfsService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit() {
    this.permissions = this.authStorageService.getPermissions();
    const state = history.state;
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

    this.tableActions = [
      {
        name: $localize`Add mirroring path`,
        permission: 'create',
        icon: this.icons.add,
        click: () => this.openAddPathPanel()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  openAddPathPanel() {
    this.addPathPanelExpanded = true;
    this.addPathForm.reset({
      selectedSubvolume: '',
      snapshotScheduleOption: 'existing'
    });
  }

  closeAddPathPanel() {
    this.addPathPanelExpanded = false;
  }
}
