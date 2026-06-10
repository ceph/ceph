import { Component, OnInit, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subject, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Daemon, Filesystem, MirroringRow, Peer } from '~/app/shared/models/cephfs.model';

@Component({
  selector: 'cd-cephfs-mirroring-list',
  templateUrl: './cephfs-mirroring-list.component.html',
  styleUrls: ['./cephfs-mirroring-list.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringListComponent implements OnInit {
  @ViewChild('table', { static: true }) table: TableComponent;

  columns: CdTableColumn[];
  selection = new CdTableSelection();

  private subject$ = new Subject<void>();

  daemonStatus$ = this.subject$.pipe(
    switchMap(() =>
      this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[])))
    ),
    map((daemons) => this.buildRows(daemons))
  );

  constructor(private cephfsService: CephfsService) {}

  ngOnInit() {
    this.columns = [
      { name: $localize`Filesystem`, prop: 'local_fs_name', flexGrow: 2 },
      { name: $localize`Destination cluster`, prop: 'remote_cluster_name', flexGrow: 2 },
      { name: $localize`Mirroring status`, prop: 'mirroring_status', flexGrow: 2 },
      { name: $localize`Bytes replicated`, prop: 'bytes_replicated', flexGrow: 2 },
      { name: $localize`Last sync`, prop: 'last_sync', flexGrow: 2 },
      { name: $localize`Replicated paths`, prop: 'directory_count', flexGrow: 2 }
    ];
  }

  loadDaemonStatus() {
    this.subject$.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
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
      peerId: '-',
      id: `${daemon.daemon_id}-${fs.filesystem_id}`
    };
  }
}
