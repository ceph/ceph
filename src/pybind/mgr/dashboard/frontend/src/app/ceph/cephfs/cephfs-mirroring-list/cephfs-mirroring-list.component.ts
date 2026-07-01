import { Component, inject, OnDestroy, OnInit, ViewChild, ViewEncapsulation } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { Subject, of } from 'rxjs';
import { catchError, filter, map, switchMap, takeUntil } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { Icons } from '~/app/shared/enum/icons.enum';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Daemon, Filesystem, MirroringRow, Peer } from '~/app/shared/models/cephfs.model';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { MirroringJumpInTile } from './cephfs-mirroring-list.model';

@Component({
  selector: 'cd-cephfs-mirroring-list',
  templateUrl: './cephfs-mirroring-list.component.html',
  styleUrls: ['./cephfs-mirroring-list.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringListComponent implements OnInit, OnDestroy {
  @ViewChild('table', { static: true }) table: TableComponent;

  private cephfsService = inject(CephfsService);
  private authStorageService = inject(AuthStorageService);
  private router = inject(Router);

  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  isSetupModalOpen = false;
  selection = new CdTableSelection();
  permission = this.authStorageService.getPermissions().cephfsMirror;
  isPrepareModalOpen = false;
  jumpInTiles: MirroringJumpInTile[] = [];

  private subject$ = new Subject<void>();
  private destroy$ = new Subject<void>();
  private previousUrl = '';

  daemonStatus$ = this.subject$.pipe(
    switchMap(() =>
      this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[])))
    ),
    map((daemons) => this.buildRows(daemons))
  );

  ngOnInit(): void {
    this.jumpInTiles = this.buildJumpInTiles();
    this.columns = [
      {
        name: $localize`Filesystem`,
        prop: 'local_fs_name',
        flexGrow: 2,
        cellTransformation: CellTemplate.redirect,
        customTemplateConfig: {
          redirectLink: [CEPHFS_MIRRORING_URL, '::prop', 'overview']
        }
      },
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
    this.previousUrl = this.router.url;
    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        takeUntil(this.destroy$)
      )
      .subscribe((event) => {
        const hadModal = this.previousUrl.includes('(modal:');
        const hasModal = event.urlAfterRedirects.includes('(modal:');
        if (hadModal && !hasModal) {
          this.loadDaemonStatus();
        }
        this.previousUrl = event.urlAfterRedirects;
      });
    this.loadDaemonStatus();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  loadDaemonStatus(): void {
    this.subject$.next();
  }

  openPrepareToReceive(): void {
    this.isPrepareModalOpen = true;
  }

  closePrepareModal(): void {
    this.isPrepareModalOpen = false;
    this.loadDaemonStatus();
  }

  onTokenGenerated(): void {
    this.loadDaemonStatus();
  }

  openSetupMirroring(): void {
    this.isSetupModalOpen = true;
  }

  closeSetupModal(): void {
    this.isSetupModalOpen = false;
    this.loadDaemonStatus();
  }

  openAddPath(): void {
    const selected = this.selection.first();
    if (!selected?.filesystem_id || !selected?.local_fs_name) {
      return;
    }

    this.router.navigate([
      CEPHFS_MIRRORING_URL,
      {
        outlets: {
          modal: ['add-path', selected.filesystem_id, encodeURIComponent(selected.local_fs_name)]
        }
      }
    ]);
  }

  private buildJumpInTiles(): MirroringJumpInTile[] {
    return [
      {
        title: $localize`Set up mirroring`,
        description: $localize`Configure mirroring for a filesystem by importing a token from a peer cluster and adding paths to replicate.`,
        icon: 'replicate',
        action: () => this.openSetupMirroring()
      },
      {
        title: $localize`Prepare to receive`,
        description: $localize`Generate a bootstrap token for a filesystem to allow a peer cluster to replicate data to it.`,
        icon: 'share',
        action: () => this.openPrepareToReceive()
      }
    ];
  }

  private buildRows(daemons: Daemon[]): MirroringRow[] {
    const rows: MirroringRow[] = [];
    if (!daemons?.length) {
      return rows;
    }

    for (const daemon of daemons) {
      if (!daemon?.filesystems) {
        continue;
      }
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
