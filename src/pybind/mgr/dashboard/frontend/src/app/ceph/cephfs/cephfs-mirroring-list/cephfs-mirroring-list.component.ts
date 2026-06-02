import { Component, OnInit } from '@angular/core';
import { Subject, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { Daemon, Filesystem, MirroringRow, Peer } from '~/app/shared/models/cephfs.model';

export const MIRRORING_PATH = 'cephfs/mirroring';
@Component({
  selector: 'cd-cephfs-mirroring-list',
  templateUrl: './cephfs-mirroring-list.component.html',
  styleUrls: ['./cephfs-mirroring-list.component.scss'],
  standalone: false,
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(MIRRORING_PATH) }]
})
export class CephfsMirroringListComponent implements OnInit {
  columns: CdTableColumn[];
  isSetupMirroringOpen = false;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    private urlBuilder: URLBuilderService
  ) {
    this.permission = this.authStorageService.getPermissions().cephfs;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Remote cluster`,
        prop: 'remote_cluster_name',
        flexGrow: 2
      },
      { name: $localize`Local filesystem`, prop: 'local_fs_name', flexGrow: 2 },
      { name: $localize`Remote filesystem`, prop: 'fs_name', flexGrow: 2 },
      { name: $localize`Remote client`, prop: 'client_name', flexGrow: 2 },
      { name: $localize`Snapshot directories`, prop: 'directory_count', flexGrow: 1 }
    ];

    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    this.tableActions = [createAction];
    this.daemonStatus$ = this.subject$.pipe(
      switchMap(() =>
        this.cephfsService.listDaemonStatus()?.pipe(
          switchMap((daemons: Daemon[]) => {
            const result: MirroringRow[] = [];

            daemons.forEach((d) => {
              d.filesystems.forEach((fs) => {
                if (!fs.peers || fs.peers.length === 0) {
                  result.push({
                    remote_cluster_name: '-',
                    local_fs_name: fs.name,
                    fs_name: fs.name,
                    client_name: '-',
                    directory_count: fs.directory_count,
                    peerId: '-',
                    id: `${d.daemon_id}-${fs.filesystem_id}`
                  });
                } else {
                  fs.peers.forEach((peer) => {
                    result.push({
                      remote_cluster_name: peer.remote.cluster_name,
                      local_fs_name: fs.name,
                      fs_name: peer.remote.fs_name,
                      client_name: peer.remote.client_name,
                      directory_count: fs.directory_count,
                      id: `${d.daemon_id}-${fs.filesystem_id}`
                    });
                  });
                }
              });
            });
            return of(result);
          }),
          catchError(() => {
            this.context?.error();
            return of(null);
          })
        )
      )
    );

    this.loadDaemonStatus();
  }

  loadDaemonStatus() {
    this.subject$.next([]);
  }

  openSetupMirroring() {
    this.isSetupMirroringOpen = true;
  }

  closeSetupModal() {
    this.isSetupMirroringOpen = false;
    this.loadDaemonStatus();
  }

  onMirroringSetupComplete(_event: any) {
    this.isSetupMirroringOpen = false;
    this.loadDaemonStatus();
  }
}
