import { Component, OnInit, ViewChild } from '@angular/core';
import { Subject, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Daemon, Filesystem, MirroringRow, Peer } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-mirroring-list',
  templateUrl: './cephfs-mirroring-list.component.html',
  styleUrls: ['./cephfs-mirroring-list.component.scss'],
  standalone: false
})
export class CephfsMirroringListComponent implements OnInit {
  @ViewChild('table', { static: true }) table: TableComponent;

  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  isPrepareModalOpen = false;
  isAddPathOpen = false;
  addPathFsName = '';
  addPathFsId = 0;
  addPathDestCluster = '';
  addPathDestFs = '';

  private subject$ = new Subject<void>();

  daemonStatus$ = this.subject$.pipe(
    switchMap(() =>
      this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[])))
    ),
    map((daemons) => this.buildRows(daemons))
  );

  constructor(
    private cephfsService: CephfsService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().cephfs;
  }

  ngOnInit() {
    this.columns = [
      { name: $localize`Filesystem`, prop: 'local_fs_name', flexGrow: 2 },
      { name: $localize`Destination cluster`, prop: 'remote_cluster_name', flexGrow: 2 },
      { name: $localize`Mirroring status`, prop: 'mirroring_status', flexGrow: 2 },
      { name: $localize`Bytes replicated`, prop: 'bytes_replicated', flexGrow: 2 },
      { name: $localize`Last sync`, prop: 'last_sync', flexGrow: 2 },
      { name: $localize`Replicated paths`, prop: 'directory_count', flexGrow: 2 }
    ];
    this.tableActions = [
      {
        name: $localize`Add mirror path`,
        permission: 'update',
        icon: Icons.add,
        click: () => this.openAddPath(),
        disable: (selection: CdTableSelection) => !selection.hasSingleSelection
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  loadDaemonStatus() {
    this.subject$.next();
  }


  openAddPath() {
    const selected = this.selection.first();
    if (selected) {
      this.addPathFsName = selected.local_fs_name;
      this.addPathFsId = selected.filesystem_id ?? 0;
      this.addPathDestCluster = selected.remote_cluster_name !== '-' ? selected.remote_cluster_name : '';
      this.addPathDestFs = selected.fs_name !== '-' ? selected.fs_name : '';
      this.isAddPathOpen = true;
    }
  }

  onPathsAdded(_paths: string[]) {
    this.isAddPathOpen = false;
    this.loadDaemonStatus();
  }

  closeAddPathTearsheet() {
    this.isAddPathOpen = false;
  }

  private buildRows(daemons: Daemon[]): MirroringRow[] {
    const rows: MirroringRow[] = [];
    if (!daemons?.length) {
      return rows;
    }

    for (const daemon of daemons) {
      if (!daemon?.filesystems) continue;
      for (const fs of daemon.filesystems) {
        if (fs.peers?.length) {
          for (const peer of fs.peers) {
            rows.push(this.peerToRow(daemon, fs, peer));
          }
        } else {
          rows.push(this.noPeerRow(daemon, fs));
        }
      }
    }
    return rows;
  }

  private peerToRow(daemon: Daemon, fs: Filesystem, peer: Peer): MirroringRow {
    return {
      remote_cluster_name: peer.remote?.cluster_name ?? '-',
      local_fs_name: fs.name,
      fs_name: peer.remote?.fs_name ?? '-',
      client_name: peer.remote?.client_name ?? '-',
      directory_count: fs.directory_count ?? 0,
      filesystem_id: fs.filesystem_id,
      id: `${daemon.daemon_id}-${fs.filesystem_id}`
    };
  }

  private noPeerRow(daemon: Daemon, fs: Filesystem): MirroringRow {
    return {
      remote_cluster_name: '-',
      local_fs_name: fs.name,
      fs_name: fs.name,
      client_name: '-',
      directory_count: fs.directory_count ?? 0,
      filesystem_id: fs.filesystem_id,
      peerId: '-',
      id: `${daemon.daemon_id}-${fs.filesystem_id}`
    };
  }
}
