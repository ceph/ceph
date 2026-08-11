import {
  Component,
  inject,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { forkJoin, Observable, of, Subject, Subscriber } from 'rxjs';
import { catchError, filter, map, switchMap, takeUntil } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { MirroringSyncStatus } from '~/app/shared/enum/cephfs-mirroring-sync-status.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import {
  CONFIRM_DISABLE,
  CONFIRM_DISABLE_MESSAGE,
  Daemon,
  Filesystem,
  hasPendingReplication,
  MirroringRow,
  MirrorStatusResponse,
  Peer
} from '~/app/shared/models/cephfs.model';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { MirroringSyncUtils } from '../mirroring-sync-utils';
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
  @ViewChild('disableMirroringTpl', { static: true })
  disableMirroringTpl: TemplateRef<any>;

  private cephfsService = inject(CephfsService);
  private authStorageService = inject(AuthStorageService);
  private modalService = inject(ModalCdsService);
  private taskWrapper = inject(TaskWrapperService);
  private router = inject(Router);
  private relativeDatePipe = inject(RelativeDatePipe);

  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  isSetupModalOpen = false;
  selection = new CdTableSelection();
  permission = this.authStorageService.getPermissions().cephfsMirror;
  isPrepareModalOpen = false;
  jumpInTiles: MirroringJumpInTile[] = [];
  MirroringSyncStatus = MirroringSyncStatus;

  private subject$ = new Subject<void>();
  private destroy$ = new Subject<void>();
  private previousUrl = '';

  daemonStatus$ = this.subject$.pipe(
    switchMap(() =>
      this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[])))
    ),
    switchMap((daemons) => this.enrichRowsWithSyncInfo(daemons))
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
      { name: $localize`Site name`, prop: 'remote_site_name', flexGrow: 2 },
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
      },
      {
        name: $localize`Disable mirroring`,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.disableMirroringModal(),
        disable: (selection: CdTableSelection) => !selection.hasSingleSelection,
        canBePrimary: () => false
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

  disableMirroringModal(): void {
    const row = this.selection.first() as MirroringRow;
    const fsName = row.local_fs_name;
    const peerUuid = row.peer_uuid;

    const status$ = peerUuid
      ? this.cephfsService
          .getMirrorStatus(fsName, undefined, peerUuid)
          .pipe(catchError(() => of(null)))
      : of(null);

    status$.subscribe((status) => {
      const pendingReplication = hasPendingReplication(status, peerUuid);

      this.openDisableMirroringModal(row, fsName, pendingReplication);
    });
  }

  private openDisableMirroringModal(
    row: MirroringRow,
    fsName: string,
    hasPendingReplicationFlag: boolean
  ): void {
    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: $localize`mirroring`,
      itemNames: [fsName],
      actionDescription: $localize`disable`,
      bodyTemplate: this.disableMirroringTpl,
      bodyContext: {
        row,
        confirmHeading: CONFIRM_DISABLE,
        deletionMessage: CONFIRM_DISABLE_MESSAGE,
        hasPendingReplication: hasPendingReplicationFlag
      },
      submitText: $localize`Disable`,
      submitActionObservable: () =>
        new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('cephfs/mirroring/disable', { fsName }),
              call: this.cephfsService.disableMirror(fsName)
            })
            .subscribe({
              error: (resp) => observer.error(resp),
              complete: () => {
                this.loadDaemonStatus();
                observer.complete();
              }
            });
        })
    });
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

  private enrichRowsWithSyncInfo(daemons: Daemon[]): Observable<MirroringRow[]> {
    const rows = this.buildRows(daemons);
    if (!rows.length) {
      return of(rows);
    }

    return forkJoin(
      rows.map((row) =>
        row.local_fs_name && row.peer_uuid
          ? this.cephfsService.getMirrorStatus(row.local_fs_name, undefined, row.peer_uuid).pipe(
              catchError(() => of({} as MirrorStatusResponse)),
              map((status) => this.applySyncInfo(row, status))
            )
          : of(this.applySyncInfo(row, null))
      )
    );
  }

  private applySyncInfo(row: MirroringRow, status: MirrorStatusResponse | null): MirroringRow {
    const sync = status
      ? MirroringSyncUtils.extractLatestSync(status)
      : { info: MirroringSyncUtils.emptySyncInfo() };

    return {
      ...row,
      bytes_replicated: sync.info.bytesSynced,
      last_sync: sync.info.syncedAt ? this.relativeDatePipe.transform(sync.info.syncedAt) : '-'
    };
  }

  private buildRows(daemons: Daemon[]): MirroringRow[] {
    // Multiple cephfs-mirror daemons report the same FS/peer topology with
    // per-daemon counters (directory_count, failure/recovery) that must be summed.
    const rowsByKey = new Map<string, MirroringRow>();
    if (!daemons?.length) {
      return [];
    }

    for (const daemon of daemons) {
      if (!daemon?.filesystems) {
        continue;
      }
      for (const fs of daemon.filesystems) {
        if (!fs.peers?.length) {
          continue;
        }
        for (const peer of fs.peers) {
          if (!this.hasPeerInfo(peer)) {
            continue;
          }
          const key = `${fs.filesystem_id}-${peer.uuid}`;
          const existing = rowsByKey.get(key);
          if (existing) {
            this.aggregatePeerStats(existing, fs, peer);
          } else {
            rowsByKey.set(key, this.peerToRow(fs, peer));
          }
        }
      }
    }
    return Array.from(rowsByKey.values());
  }

  private hasPeerInfo(peer: Peer): boolean {
    const remote = peer?.remote;
    return !!(remote?.cluster_name || remote?.fs_name || remote?.client_name);
  }

  private aggregatePeerStats(row: MirroringRow, fs: Filesystem, peer: Peer): void {
    row.directory_count += fs.directory_count ?? 0;
    row.failure_count = (row.failure_count ?? 0) + (peer.stats?.failure_count ?? 0);
    row.recovery_count = (row.recovery_count ?? 0) + (peer.stats?.recovery_count ?? 0);
    this.applySyncStatus(row, row.failure_count);
  }

  private peerToRow(fs: Filesystem, peer: Peer): MirroringRow {
    const failureCount = peer.stats?.failure_count ?? 0;
    const recoveryCount = peer.stats?.recovery_count ?? 0;
    const row: MirroringRow = {
      remote_site_name: peer.remote?.cluster_name ?? '-',
      local_fs_name: fs.name,
      fs_name: peer.remote?.fs_name ?? '-',
      client_name: peer.remote?.client_name ?? '-',
      directory_count: fs.directory_count ?? 0,
      filesystem_id: fs.filesystem_id,
      peer_uuid: peer.uuid,
      failure_count: failureCount,
      recovery_count: recoveryCount,
      id: `${fs.filesystem_id}-${peer.uuid}`
    };
    this.applySyncStatus(row, failureCount);
    return row;
  }

  private applySyncStatus(row: MirroringRow, failureCount: number): void {
    row.sync_status = failureCount > 0 ? MirroringSyncStatus.ERROR : MirroringSyncStatus.SYNCING;
    row.sync_status_label = failureCount > 0 ? $localize`Error` : $localize`Syncing`;
  }
}
