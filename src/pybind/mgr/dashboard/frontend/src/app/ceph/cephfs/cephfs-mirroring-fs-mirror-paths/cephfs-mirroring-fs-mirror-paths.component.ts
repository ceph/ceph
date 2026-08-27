import {
  Component,
  OnInit,
  OnDestroy,
  TemplateRef,
  ViewChild,
  ViewEncapsulation,
  inject
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, forkJoin, of, Subscription } from 'rxjs';
import { catchError, map, shareReplay, switchMap, tap } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshot } from '~/app/shared/models/cephfs-directory-models';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import {
  MirroringSnapshotSection,
  MirroringSnapshotStatus,
  MirroringSyncStatus
} from '~/app/shared/enum/cephfs-mirroring-sync-status.enum';
import { Icons, ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { FormatterService } from '~/app/shared/services/formatter.service';
import {
  MirrorDirStatus,
  MirrorCheckpoint,
  MirrorCheckpointStatus,
  MirrorStatusResponse
} from '~/app/shared/models/cephfs.model';
import { MirrorPathSchedule } from '~/app/shared/models/snapshot-schedule';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { MirroringSyncUtils } from '../mirroring-sync-utils';

type SyncStatus = 'syncing' | 'idle' | 'failed' | 'completed';

interface SnapshotEntry {
  name: string;
  status: MirroringSnapshotStatus;
  eta?: string;
  icon: keyof typeof ICON_TYPE;
  iconClass: string;
  statusLabel: string;
  filesSynced?: number;
  bytesSynced?: number;
  created?: string;
}

interface SnapshotPanelViewModel extends SnapshotEntry {
  expanded: boolean;
  hasCheckpoint: boolean;
  checkpoint?: MirrorCheckpoint;
  replicationStatusLabel: string;
}

interface MirrorPath {
  path: string;
  syncStatus: SyncStatus;
  syncStatusIcon: keyof typeof ICON_TYPE;
  syncStatusClass: string;
  currentSyncSnapshot: string;
  currentSyncEta?: string;
  currentSyncMode?: string;
  lastSyncedSnapshot: string;
  lastSyncedTime?: string;
  snapshotCount?: number;
  pendingSnapshotCount?: number;
  snapshots?: SnapshotEntry[];
  checkpointCount?: number;
  renamedSnapshotCount?: number;
  syncProgress?: number;
  filesSynced?: number;
  totalFiles?: number;
  bytesSynced?: number;
  totalBytes?: number;
  crawlState?: string;
  crawlDuration?: string;
  datasyncQueueWaitState?: string;
  datasyncQueueWaitDuration?: string;
  avgReadThroughput?: string;
  avgWriteThroughput?: string;
}

const SYNC_STATUS_ICONS: Record<SyncStatus, keyof typeof ICON_TYPE> = {
  syncing: 'inProgress',
  idle: 'pendingFilled',
  failed: 'danger',
  completed: 'checkMarkOutline'
};

const SYNC_STATUS_CLASSES: Record<SyncStatus, string> = {
  syncing: 'info',
  completed: 'success',
  idle: 'muted',
  failed: 'danger'
};

const SNAPSHOT_STATUS_ICONS: Record<MirroringSnapshotStatus, keyof typeof ICON_TYPE> = {
  [MirroringSnapshotStatus.IN_PROGRESS]: 'inProgress',
  [MirroringSnapshotStatus.REPLICATED]: 'checkMarkOutline',
  [MirroringSnapshotStatus.PENDING]: 'pendingFilled',
  [MirroringSnapshotStatus.FAILED]: 'danger'
};

const SNAPSHOT_STATUS_CLASSES: Record<MirroringSnapshotStatus, string> = {
  [MirroringSnapshotStatus.IN_PROGRESS]: 'info',
  [MirroringSnapshotStatus.REPLICATED]: 'success',
  [MirroringSnapshotStatus.PENDING]: 'muted',
  [MirroringSnapshotStatus.FAILED]: 'danger'
};

@Component({
  selector: 'cd-cephfs-mirroring-fs-mirror-paths',
  templateUrl: './cephfs-mirroring-fs-mirror-paths.component.html',
  styleUrls: ['./cephfs-mirroring-fs-mirror-paths.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringFsMirrorPathsComponent implements OnInit, OnDestroy {
  @ViewChild('syncStatusTpl', { static: true })
  syncStatusTpl!: TemplateRef<unknown>;

  @ViewChild('pathTpl', { static: true })
  pathTpl!: TemplateRef<unknown>;

  @ViewChild('currentSyncSnapshotTpl', { static: true })
  currentSyncSnapshotTpl!: TemplateRef<unknown>;

  private cephfsService = inject(CephfsService);
  private snapshotScheduleService = inject(CephfsSnapshotScheduleService);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private formatterService = inject(FormatterService);
  private authStorageService = inject(AuthStorageService);
  private cdsModalService = inject(ModalCdsService);
  private notificationService = inject(NotificationService);
  private taskWrapper = inject(TaskWrapperService);
  private relativeDatePipe = inject(RelativeDatePipe);

  columns: CdTableColumn[] = [];
  mirrorPaths: MirrorPath[] = [];
  selection = new CdTableSelection();
  tableActions: CdTableAction[] = [];
  permission = this.authStorageService.getPermissions().cephfsMirror;
  selectedPath: MirrorPath | null = null;
  sidePanelOpen = false;
  fsName: string = '';
  schedulePolicies: MirrorPathSchedule[] = [];
  schedulePoliciesLoading = false;
  removingSchedule = '';
  currentSnapshotPanels: SnapshotPanelViewModel[] = [];
  syncedSnapshotPanels: SnapshotPanelViewModel[] = [];
  pathCheckpoints: MirrorCheckpoint[] = [];
  pathSnapshots: CephfsSnapshot[] = [];
  pathSnapshotsLoading = false;
  checkpointActionInProgress = '';
  expandedSnapshotNames = new Set<string>();

  private subscriptions = new Subscription();
  private mirrorPathsSubscription?: Subscription;
  private pathSnapshotsLoadedFor: string | null = null;
  private readonly snapshotDetailsQuery$ = new BehaviorSubject<{
    path: string | null;
    gen: number;
  }>({ path: null, gen: 0 });

  readonly snapshotDetails$ = this.snapshotDetailsQuery$.pipe(
    switchMap(({ path }) => {
      if (!path || !this.fsName) {
        this.pathCheckpoints = [];
        this.pathSnapshots = [];
        this.pathSnapshotsLoadedFor = null;
        this.pathSnapshotsLoading = false;
        this.refreshSnapshotPanels();
        return of({ loading: false });
      }

      return forkJoin({
        checkpoints: this.cephfsService
          .listMirrorCheckpoints(this.fsName, path)
          .pipe(catchError(() => of({ checkpoints: [] as MirrorCheckpoint[] }))),
        snapshots: this.cephfsService
          .listMirrorPathSnapshots(this.fsName, path)
          .pipe(catchError(() => of([] as CephfsSnapshot[])))
      }).pipe(
        tap(({ checkpoints, snapshots }) => {
          if (this.selectedPath?.path !== path) {
            return;
          }
          this.pathCheckpoints = checkpoints.checkpoints ?? [];
          this.pathSnapshots = snapshots;
          this.pathSnapshotsLoadedFor = path;
          this.selectedPath.checkpointCount = this.pathCheckpoints.length;
          this.pathSnapshotsLoading = false;
          this.refreshSnapshotPanels();
        }),
        map(() => ({ loading: false })),
        catchError(() => {
          if (this.selectedPath?.path === path) {
            this.pathSnapshotsLoading = false;
            this.refreshSnapshotPanels();
          }
          return of({ loading: false });
        })
      );
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  get snapshotPanels(): SnapshotPanelViewModel[] {
    return [...this.currentSnapshotPanels, ...this.syncedSnapshotPanels];
  }

  get hasSnapshotPanels(): boolean {
    return this.snapshotPanels.length > 0;
  }

  ngOnInit(): void {
    this.initializeColumns();
    this.initializeTableActions();
    this.subscriptions.add(this.snapshotDetails$.subscribe());
    this.fetchFsName();
  }

  ngOnDestroy(): void {
    this.snapshotDetailsQuery$.complete();
    this.subscriptions.unsubscribe();
  }

  initializeColumns(): void {
    this.columns = [
      {
        name: $localize`Path`,
        prop: 'path',
        flexGrow: 2,
        cellTemplate: this.pathTpl,
        sortable: true
      },
      {
        name: $localize`Sync status`,
        prop: 'syncStatus',
        flexGrow: 1.5,
        cellTemplate: this.syncStatusTpl,
        sortable: true
      },
      {
        name: $localize`Snapshots synced`,
        prop: 'snapshotCount',
        flexGrow: 1.5
      },
      {
        name: $localize`Current sync snapshot`,
        prop: 'currentSyncSnapshot',
        flexGrow: 1.5,
        cellTemplate: this.currentSyncSnapshotTpl,
        sortable: true
      },
      {
        name: $localize`Last synced snapshot`,
        prop: 'lastSyncedSnapshot',
        flexGrow: 1.5,
        sortable: true
      }
    ];
  }

  initializeTableActions(): void {
    this.tableActions = [
      {
        name: $localize`Add mirror path`,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openAddPath()
      },
      {
        name: $localize`Remove path`,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removePathModal(),
        disable: (selection: CdTableSelection) => !selection.hasSingleSelection
      }
    ];
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  openAddPath(): void {
    if (!this.fsName) {
      return;
    }

    this.subscriptions.add(
      this.cephfsService.list().subscribe({
        next: (filesystems: { id?: number; mdsmap?: { fs_name?: string } }[]) => {
          const fsId = filesystems.find((fs) => fs.mdsmap?.fs_name === this.fsName)?.id ?? 0;
          const encodedFsName = encodeURIComponent(this.fsName);
          // Absolute URL avoids NG04006 when leaving /mirroring/:fsName for the list modal outlet
          this.router.navigateByUrl(
            `${CEPHFS_MIRRORING_URL}/(modal:add-path/${fsId}/${encodedFsName})`,
            {
              state: {
                returnUrl: `${CEPHFS_MIRRORING_URL}/${encodedFsName}/mirror-paths`
              }
            }
          );
        },
        error: () => {
          this.notificationService.show(
            NotificationType.error,
            $localize`Error`,
            $localize`Failed to load filesystems for adding a mirror path.`
          );
        }
      })
    );
  }

  removePathModal(): void {
    const path = this.selection.first().path;
    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.medium,
      itemDescription: $localize`mirror path`,
      itemNames: [path],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/mirroring/path/remove', {
            fsName: this.fsName,
            path
          }),
          call: this.cephfsService.removeMirrorDirectory(this.fsName, path).pipe(
            tap(() => {
              if (this.selectedPath?.path === path) {
                this.closeSidePanel();
              }
              this.loadMirrorPaths();
            })
          )
        })
    });
  }

  private fetchFsName(): void {
    this.subscriptions.add(
      this.route.parent?.paramMap.subscribe((paramMap) => {
        this.fsName = paramMap.get('fsName') || '';
        if (this.fsName) {
          this.loadMirrorPaths();
        }
      }) || new Subscription()
    );
  }

  loadMirrorPaths(): void {
    if (!this.fsName) {
      return;
    }

    this.mirrorPathsSubscription?.unsubscribe();
    this.mirrorPathsSubscription = this.cephfsService.getMirrorStatus(this.fsName).subscribe({
      next: (data: MirrorStatusResponse) => {
        this.mirrorPaths = this.parseMirrorStatus(data);
        if (this.selectedPath) {
          this.selectedPath =
            this.mirrorPaths.find((mirrorPath) => mirrorPath.path === this.selectedPath?.path) ??
            null;
          this.sidePanelOpen = !!this.selectedPath;
          if (this.selectedPath) {
            this.refreshSnapshotPanels();
            if (!this.pathSnapshotsLoading) {
              this.loadSnapshotDetails(this.selectedPath.path, true);
            }
          } else {
            this.clearSnapshotDetails();
          }
        }
      },
      error: () => {
        this.mirrorPaths = [];
        this.selectedPath = null;
        this.sidePanelOpen = false;
        this.clearSnapshotDetails();
      }
    });
    this.subscriptions.add(this.mirrorPathsSubscription);
  }

  parseMirrorStatus(data: MirrorStatusResponse): MirrorPath[] {
    if (!data?.metrics) {
      return [];
    }

    const paths: MirrorPath[] = [];

    for (const path in data.metrics) {
      if (Object.prototype.hasOwnProperty.call(data.metrics, path)) {
        const pathData = data.metrics[path];

        // Skip invalid entries
        if (!pathData?.peer) {
          continue;
        }

        const peerInfo = this.extractPeerInfo(pathData);
        if (!peerInfo) continue;

        paths.push(this.buildMirrorPath(path, peerInfo));
      }
    }

    return paths;
  }

  private parsePercent(percent: string | undefined): number {
    if (!percent) {
      return 0;
    }

    const match = percent.match(/([\d.]+)%/);
    return match ? Math.round(parseFloat(match[1])) : 0;
  }

  private parseByteValue(value: string | undefined): number {
    if (!value) {
      return 0;
    }

    const normalizedValue = value.replace(/\s+/g, '');
    return this.formatterService.toBytes(normalizedValue, 0) ?? 0;
  }

  private calculateSyncProgress(
    fileProgress: number,
    filesSynced: number,
    totalFiles: number,
    bytesSynced: number,
    totalBytes: number,
    byteProgress: number
  ): number {
    if (fileProgress > 0) return fileProgress;
    if (totalFiles > 0) return Math.round((filesSynced / totalFiles) * 100);
    if (byteProgress > 0) return byteProgress;
    if (totalBytes > 0) return Math.round((bytesSynced / totalBytes) * 100);
    return 0;
  }

  private extractPeerInfo(pathData: {
    peer?: Record<string, MirrorDirStatus>;
  }): MirrorDirStatus | null {
    const peerEntries = Object.entries(pathData.peer ?? {});
    return peerEntries.length > 0 ? peerEntries[0][1] : null;
  }

  private buildMirrorPath(path: string, peerInfo: MirrorDirStatus): MirrorPath {
    const currentSnap = peerInfo.current_syncing_snap ?? peerInfo.current_sync_snap;
    const filesSynced = currentSnap?.files?.sync_files ?? 0;
    const totalFiles = currentSnap?.files?.total_files ?? 0;
    const fileProgress = this.parsePercent(currentSnap?.files?.sync_percent);
    const bytesSynced = this.parseByteValue(currentSnap?.bytes?.sync_bytes);
    const totalBytes = this.parseByteValue(currentSnap?.bytes?.total_bytes);
    const byteProgress = this.parsePercent(currentSnap?.bytes?.sync_percent);

    const syncProgress = this.calculateSyncProgress(
      fileProgress,
      filesSynced,
      totalFiles,
      bytesSynced,
      totalBytes,
      byteProgress
    );

    const syncStatus = (peerInfo.state ?? 'idle') as SyncStatus;
    const snapshots = this.buildSnapshotList(peerInfo, syncStatus);
    const lastSyncedAt = MirroringSyncUtils.parseSyncTimeStamp(
      peerInfo.last_synced_snap?.sync_time_stamp
    );

    return {
      path,
      syncStatus,
      syncStatusIcon: SYNC_STATUS_ICONS[syncStatus] ?? 'infoCircle',
      syncStatusClass: SYNC_STATUS_CLASSES[syncStatus] ?? '',
      currentSyncSnapshot: currentSnap?.name ?? '-',
      currentSyncEta: currentSnap?.eta,
      currentSyncMode: currentSnap?.['sync-mode'],
      lastSyncedSnapshot: peerInfo.last_synced_snap?.name ?? '-',
      lastSyncedTime:
        lastSyncedAt != null ? this.relativeDatePipe.transform(lastSyncedAt) : undefined,
      snapshotCount: peerInfo.snaps_synced ?? 0,
      pendingSnapshotCount: snapshots.filter((snapshot) =>
        this.isOpenSnapshotStatus(snapshot.status)
      ).length,
      snapshots,
      renamedSnapshotCount: peerInfo.snaps_renamed ?? 0,
      syncProgress,
      filesSynced,
      totalFiles,
      bytesSynced,
      totalBytes,
      crawlState: currentSnap?.crawl?.state,
      crawlDuration: currentSnap?.crawl?.duration,
      datasyncQueueWaitState: currentSnap?.datasync_queue_wait?.state,
      datasyncQueueWaitDuration: currentSnap?.datasync_queue_wait?.duration,
      avgReadThroughput: currentSnap?.avg_read_throughput_bytes,
      avgWriteThroughput: currentSnap?.avg_write_throughput_bytes
    };
  }

  private buildSnapshotList(peerInfo: MirrorDirStatus, syncStatus: SyncStatus): SnapshotEntry[] {
    const snapshots: SnapshotEntry[] = [];
    const currentSnap = peerInfo.current_syncing_snap ?? peerInfo.current_sync_snap;
    const lastSnap = peerInfo.last_synced_snap;
    const currentName = currentSnap?.name;
    const lastName = lastSnap?.name;

    if (currentName && currentName !== '-') {
      if (syncStatus === MirroringSyncStatus.SYNCING) {
        snapshots.push(
          this.createSnapshotEntry({
            name: currentName,
            status: MirroringSnapshotStatus.IN_PROGRESS,
            eta: currentSnap?.eta,
            filesSynced: currentSnap?.files?.sync_files,
            bytesSynced:
              currentSnap?.bytes?.sync_bytes != null
                ? this.parseByteValue(String(currentSnap.bytes.sync_bytes))
                : undefined
          })
        );
      } else if (currentName !== lastName) {
        snapshots.push(
          this.createSnapshotEntry({
            name: currentName,
            status:
              syncStatus === 'failed'
                ? MirroringSnapshotStatus.FAILED
                : MirroringSnapshotStatus.PENDING,
            filesSynced: currentSnap?.files?.sync_files,
            bytesSynced:
              currentSnap?.bytes?.sync_bytes != null
                ? this.parseByteValue(String(currentSnap.bytes.sync_bytes))
                : undefined
          })
        );
      }
    }

    if (lastName && lastName !== '-') {
      snapshots.push(
        this.createSnapshotEntry({
          name: lastName,
          status: MirroringSnapshotStatus.REPLICATED,
          filesSynced: lastSnap?.sync_files,
          bytesSynced:
            lastSnap?.sync_bytes != null
              ? this.parseByteValue(String(lastSnap.sync_bytes))
              : undefined
        })
      );
    }

    return snapshots;
  }

  private createSnapshotEntry(entry: {
    name: string;
    status: MirroringSnapshotStatus;
    eta?: string;
    filesSynced?: number;
    bytesSynced?: number;
    created?: string;
  }): SnapshotEntry {
    return {
      ...entry,
      icon: SNAPSHOT_STATUS_ICONS[entry.status],
      iconClass: SNAPSHOT_STATUS_CLASSES[entry.status],
      statusLabel: this.snapshotStatusLabel(entry.status)
    };
  }

  private snapshotStatusLabel(status: MirroringSnapshotStatus): string {
    switch (status) {
      case MirroringSnapshotStatus.IN_PROGRESS:
        return $localize`replication in-progress`;
      case MirroringSnapshotStatus.REPLICATED:
        return $localize`replicated.`;
      case MirroringSnapshotStatus.PENDING:
        return $localize`replication pending`;
      case MirroringSnapshotStatus.FAILED:
        return $localize`replication failed`;
      default:
        return '';
    }
  }

  get selectedPathSyncStatusIcon(): keyof typeof ICON_TYPE {
    return this.selectedPath?.syncStatusIcon ?? 'infoCircle';
  }

  get selectedPathSyncStatusClass(): string {
    return this.selectedPath?.syncStatusClass ?? '';
  }

  get selectedPathSyncStatusLabel(): string {
    return this.selectedPath?.syncStatus ? this.toTitleCase(this.selectedPath.syncStatus) : '-';
  }

  get showSelectedPathProgress(): boolean {
    return (
      this.selectedPath?.syncStatus === MirroringSyncStatus.SYNCING &&
      this.selectedPath?.syncProgress !== undefined
    );
  }

  get canMarkCheckpoint(): boolean {
    return !!this.permission?.create;
  }

  get canRemoveCheckpoint(): boolean {
    return !!this.permission?.delete;
  }

  onPathClick(path: MirrorPath): void {
    const pathChanged = this.selectedPath?.path !== path.path;
    if (pathChanged) {
      this.expandedSnapshotNames.clear();
      this.pathCheckpoints = [];
      this.pathSnapshots = [];
      this.pathSnapshotsLoadedFor = null;
    }

    this.selectedPath = path;
    this.sidePanelOpen = true;
    this.refreshSnapshotPanels();
    this.loadSchedulePolicies(path.path);
    this.loadSnapshotDetails(path.path, !pathChanged);
    this.loadMirrorPaths();
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
    this.schedulePolicies = [];
    this.schedulePoliciesLoading = false;
    this.removingSchedule = '';
    this.currentSnapshotPanels = [];
    this.syncedSnapshotPanels = [];
    this.checkpointActionInProgress = '';
    this.expandedSnapshotNames.clear();
    this.clearSnapshotDetails();
  }

  private clearSnapshotDetails(): void {
    this.pathCheckpoints = [];
    this.pathSnapshots = [];
    this.pathSnapshotsLoadedFor = null;
    this.pathSnapshotsLoading = false;
    this.snapshotDetailsQuery$.next({ path: null, gen: 0 });
    this.refreshSnapshotPanels();
  }

  loadSnapshotDetails(path: string, silent = false): void {
    if (!this.fsName || !path || !this.sidePanelOpen) {
      this.clearSnapshotDetails();
      return;
    }
    if (!silent) {
      this.pathSnapshotsLoading = this.pathSnapshotsLoadedFor !== path;
    }
    this.snapshotDetailsQuery$.next({
      path,
      gen: this.snapshotDetailsQuery$.value.gen + 1
    });
  }

  /**
   * Rebuild the Snapshots tab for the open path: merge directory snaps,
   * live daemon status, and checkpoints, then split into Current vs Already synced.
   */
  refreshSnapshotPanels(): void {
    if (!this.selectedPath) {
      this.currentSnapshotPanels = [];
      this.syncedSnapshotPanels = [];
      return;
    }

    // Live replication from mirror status (current / last snap).
    const liveByName = new Map(
      (this.selectedPath.snapshots ?? []).map((snapshot) => [snapshot.name, snapshot])
    );
    // Directory snapshots from CephFS ls_snapshots.
    const pathByName = new Map(this.pathSnapshots.map((snapshot) => [snapshot.name, snapshot]));
    // Checkpoint still gets a row if ls_snapshots no longer lists that snap.
    const checkpointByName = new Map(
      this.pathCheckpoints.map((checkpoint) => [checkpoint.snap_name, checkpoint])
    );
    const lastName = this.normalizedSnapshotName(this.selectedPath.lastSyncedSnapshot);
    const lastCreated = lastName ? pathByName.get(lastName)?.created : undefined;

    // Every snap name for this path.
    const names = new Set<string>([
      ...pathByName.keys(),
      ...liveByName.keys(),
      ...checkpointByName.keys()
    ]);
    if (lastName) {
      names.add(lastName);
    }

    const currentPanels: SnapshotPanelViewModel[] = [];
    const syncedPanels: SnapshotPanelViewModel[] = [];

    for (const name of names) {
      // Daemon-reported row for this name, if any (in-progress / pending / failed /
      // replicated). Missing when we only know the snap from ls_snapshots or a checkpoint.
      const liveSnap = liveByName.get(name);
      const created = pathByName.get(name)?.created;
      // Which list: Current (still replicating, or created after last-synced) vs Already synced.
      const section = this.snapshotSection(name, liveSnap, created, lastName, lastCreated);
      // Use the daemon row when present; otherwise infer pending vs replicated from the list.
      const entry = liveSnap
        ? { ...liveSnap, created: liveSnap.created ?? created }
        : this.createSnapshotEntry({
            name,
            status: this.snapshotStatusForSection(section),
            created
          });
      const panel = this.buildSnapshotPanel(entry, checkpointByName.get(name));
      if (section === MirroringSnapshotSection.CURRENT) {
        currentPanels.push(panel);
      } else {
        syncedPanels.push(panel);
      }
    }

    currentPanels.sort((left, right) => this.compareCurrentSnapshots(left, right));
    syncedPanels.sort((left, right) => this.compareSyncedSnapshots(left, right));

    this.currentSnapshotPanels = currentPanels;
    this.syncedSnapshotPanels = syncedPanels;
    this.selectedPath.pendingSnapshotCount = currentPanels.filter((snapshot) =>
      this.isOpenSnapshotStatus(snapshot.status)
    ).length;
  }

  private normalizedSnapshotName(name?: string): string | undefined {
    return name && name !== '-' ? name : undefined;
  }

  private snapshotSection(
    name: string,
    liveSnap: SnapshotEntry | undefined,
    created: string | undefined,
    lastName: string | undefined,
    lastCreated: string | undefined
  ): MirroringSnapshotSection {
    if (liveSnap && liveSnap.status !== MirroringSnapshotStatus.REPLICATED) {
      return MirroringSnapshotSection.CURRENT;
    }
    const waitingBehindLast =
      name !== lastName &&
      !!lastCreated &&
      !!created &&
      this.snapshotCreatedTime(created) > this.snapshotCreatedTime(lastCreated);
    return waitingBehindLast ? MirroringSnapshotSection.CURRENT : MirroringSnapshotSection.SYNCED;
  }

  private snapshotStatusForSection(section: MirroringSnapshotSection): MirroringSnapshotStatus {
    return section === MirroringSnapshotSection.CURRENT
      ? MirroringSnapshotStatus.PENDING
      : MirroringSnapshotStatus.REPLICATED;
  }

  private isOpenSnapshotStatus(status: MirroringSnapshotStatus): boolean {
    return (
      status === MirroringSnapshotStatus.IN_PROGRESS || status === MirroringSnapshotStatus.PENDING
    );
  }

  private compareCurrentSnapshots(
    left: SnapshotPanelViewModel,
    right: SnapshotPanelViewModel
  ): number {
    const statusOrder: Record<MirroringSnapshotStatus, number> = {
      [MirroringSnapshotStatus.IN_PROGRESS]: 0,
      [MirroringSnapshotStatus.PENDING]: 1,
      [MirroringSnapshotStatus.FAILED]: 2,
      [MirroringSnapshotStatus.REPLICATED]: 3
    };
    const statusDiff = statusOrder[left.status] - statusOrder[right.status];
    if (statusDiff !== 0) {
      return statusDiff;
    }
    return this.compareSyncedSnapshots(left, right);
  }

  private compareSyncedSnapshots(
    left: SnapshotPanelViewModel,
    right: SnapshotPanelViewModel
  ): number {
    const createdDiff =
      this.snapshotCreatedTime(right.created) - this.snapshotCreatedTime(left.created);
    if (createdDiff !== 0) {
      return createdDiff;
    }
    const lastName = this.normalizedSnapshotName(this.selectedPath?.lastSyncedSnapshot);
    if (lastName && left.name === lastName) {
      return -1;
    }
    if (lastName && right.name === lastName) {
      return 1;
    }
    return left.name.localeCompare(right.name);
  }

  private snapshotCreatedTime(value?: string): number {
    if (!value) {
      return 0;
    }
    const time = new Date(value).getTime();
    return Number.isNaN(time) ? 0 : time;
  }

  private buildSnapshotPanel(
    snapshot: SnapshotEntry,
    checkpoint?: MirrorCheckpoint
  ): SnapshotPanelViewModel {
    const display = checkpoint
      ? this.checkpointStatusDisplay(checkpoint.status)
      : {
          icon: snapshot.icon,
          iconClass: snapshot.iconClass,
          statusLabel: snapshot.statusLabel,
          replicationStatusLabel: this.replicationStatusLabel(snapshot.status)
        };

    return {
      ...snapshot,
      expanded: this.expandedSnapshotNames.has(snapshot.name),
      hasCheckpoint: !!checkpoint,
      checkpoint,
      icon: display.icon,
      iconClass: display.iconClass,
      statusLabel: display.statusLabel,
      replicationStatusLabel: display.replicationStatusLabel
    };
  }

  private checkpointStatusDisplay(status: MirrorCheckpointStatus): {
    icon: keyof typeof ICON_TYPE;
    iconClass: string;
    statusLabel: string;
    replicationStatusLabel: string;
  } {
    switch (status) {
      case 'created':
        return {
          icon: 'inProgress',
          iconClass: 'info',
          statusLabel: $localize`checkpoint created`,
          replicationStatusLabel: $localize`Created`
        };
      case 'complete':
        return {
          icon: 'checkMarkOutline',
          iconClass: 'success',
          statusLabel: $localize`checkpoint complete`,
          replicationStatusLabel: $localize`Complete`
        };
      case 'failed':
        return {
          icon: 'danger',
          iconClass: 'danger',
          statusLabel: $localize`checkpoint failed`,
          replicationStatusLabel: $localize`Failed`
        };
      default:
        return {
          icon: 'warning',
          iconClass: 'muted',
          statusLabel: $localize`checkpoint unknown`,
          replicationStatusLabel: $localize`Unknown`
        };
    }
  }

  toggleSnapshotExpanded(snapshotName: string): void {
    if (this.expandedSnapshotNames.has(snapshotName)) {
      this.expandedSnapshotNames.delete(snapshotName);
    } else {
      this.expandedSnapshotNames.add(snapshotName);
    }
    this.refreshSnapshotPanels();
  }

  collapseAllSnapshots(): void {
    this.expandedSnapshotNames.clear();
    this.refreshSnapshotPanels();
  }

  markAsCheckpoint(snapshot: SnapshotPanelViewModel): void {
    if (!this.fsName || !this.selectedPath?.path || !this.canMarkCheckpoint) {
      return;
    }

    const path = this.selectedPath.path;
    const snapName = snapshot.name;
    this.cdsModalService.show(ConfirmationModalComponent, {
      titleText: $localize`Mark as checkpoint`,
      buttonText: $localize`Mark as checkpoint`,
      description: $localize`Mark snapshot ${snapName} as a checkpoint?`,
      onSubmit: () => this.addCheckpoint(path, snapName)
    });
  }

  private addCheckpoint(path: string, snapName: string): void {
    this.checkpointActionInProgress = snapName;
    this.subscriptions.add(
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('cephfs/mirroring/checkpoint/add', {
            fsName: this.fsName,
            path,
            snapName
          }),
          call: this.cephfsService.addMirrorCheckpoint(this.fsName, path, snapName).pipe(
            tap(() => {
              this.checkpointActionInProgress = '';
              this.cdsModalService.dismissAll();
              this.loadSnapshotDetails(path, true);
              this.loadMirrorPaths();
            })
          )
        })
        .subscribe({
          error: () => {
            this.checkpointActionInProgress = '';
          }
        })
    );
  }

  removeCheckpointModal(snapshot: SnapshotPanelViewModel): void {
    if (!this.fsName || !this.selectedPath?.path || !this.canRemoveCheckpoint) {
      return;
    }

    const path = this.selectedPath.path;
    const snapName = snapshot.name;
    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.medium,
      itemDescription: $localize`checkpoint`,
      itemNames: [snapName],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/mirroring/checkpoint/remove', {
            fsName: this.fsName,
            path,
            snapName
          }),
          call: this.cephfsService.removeMirrorCheckpoint(this.fsName, path, snapName).pipe(
            tap(() => {
              this.loadSnapshotDetails(path, true);
              this.loadMirrorPaths();
            })
          )
        })
    });
  }

  formatSnapshotFiles(filesSynced?: number): string {
    return filesSynced === undefined ? '-' : String(filesSynced);
  }

  private replicationStatusLabel(status: MirroringSnapshotStatus): string {
    switch (status) {
      case MirroringSnapshotStatus.IN_PROGRESS:
        return $localize`In progress`;
      case MirroringSnapshotStatus.REPLICATED:
        return $localize`Replicated`;
      case MirroringSnapshotStatus.PENDING:
        return $localize`Pending`;
      case MirroringSnapshotStatus.FAILED:
        return $localize`Failed`;
      default:
        return '-';
    }
  }

  loadSchedulePolicies(path: string): void {
    if (!this.fsName || !path) {
      this.schedulePolicies = [];
      return;
    }

    this.schedulePoliciesLoading = true;
    this.subscriptions.add(
      this.snapshotScheduleService.getSnapshotSchedule(path, this.fsName, false).subscribe(
        (policies) => {
          if (this.selectedPath?.path !== path) {
            this.schedulePoliciesLoading = false;
            return;
          }

          const normalizedPath = this.normalizePath(path);
          this.schedulePolicies = policies
            .filter((policy) => {
              return (
                this.normalizePath(policy.path) === normalizedPath ||
                this.normalizePath(policy.rel_path) === normalizedPath
              );
            })
            .filter(
              (policy, index, filteredPolicies) =>
                filteredPolicies.findIndex(
                  (candidate) =>
                    candidate.path === policy.path &&
                    candidate.schedule === policy.schedule &&
                    String(candidate.start) === String(policy.start)
                ) === index
            )
            .map((policy) => this.buildSchedulePolicyViewModel(policy as MirrorPathSchedule));
          this.schedulePoliciesLoading = false;
        },
        () => {
          if (this.selectedPath?.path === path) {
            this.schedulePolicies = [];
          }
          this.schedulePoliciesLoading = false;
        }
      )
    );
  }

  removeSchedulePolicy(policy: MirrorPathSchedule): void {
    if (!policy?.path || !policy?.schedule || !policy?.start || !this.fsName) {
      return;
    }

    const retentionPolicy = policy.retention
      ? Object.entries(policy.retention)
          .filter(([, interval]) => interval !== null && interval !== undefined)
          .map(([frequency, interval]) => `${interval}-${frequency}`)
          .join('|')
      : undefined;

    this.removingSchedule = `${policy.path}@${policy.schedule}`;
    this.subscriptions.add(
      this.snapshotScheduleService
        .delete({
          path: policy.path,
          schedule: policy.schedule,
          start: policy.start,
          fs: policy.fs || this.fsName,
          retentionPolicy
        })
        .subscribe(
          () => {
            this.removingSchedule = '';
            this.loadSchedulePolicies(policy.path);
          },
          () => {
            this.removingSchedule = '';
          }
        )
    );
  }

  getScheduleStatusIcon(active: boolean): keyof typeof ICON_TYPE {
    return active ? 'success' : 'warning';
  }

  private buildSchedulePolicyViewModel(policy: MirrorPathSchedule): MirrorPathSchedule {
    const retention =
      typeof policy.retention === 'string'
        ? {}
        : ((policy.retention || {}) as Record<string, number>);
    const retentionCopy = this.buildRetentionCopy(retention);

    return {
      ...policy,
      retention,
      scheduleCopy: this.snapshotScheduleService.parseScheduleCopy(policy.schedule),
      retentionCopy,
      nextSync: this.calculateNextSync(policy),
      scheduleText: policy.schedule || '-',
      retentionText: this.formatRetentionCopy(retentionCopy),
      statusLabel: this.getScheduleStatusLabel(policy.active),
      statusIcon: this.getScheduleStatusIcon(policy.active),
      removeId: `${policy.path}@${policy.schedule}`
    };
  }

  private getScheduleStatusLabel(active: boolean): string {
    return active ? $localize`Active` : $localize`Inactive`;
  }

  private formatRetentionCopy(retentionCopy?: string[]): string {
    return retentionCopy?.length ? retentionCopy.join(', ') : '-';
  }

  private buildRetentionCopy(retention?: Record<string, number>): string[] {
    if (!retention || !Object.keys(retention).length) {
      return [];
    }

    const retentionLabels: Record<string, string> = {
      h: $localize`hourly`,
      d: $localize`daily`,
      w: $localize`weekly`,
      M: $localize`monthly`,
      m: $localize`minutely`,
      y: $localize`yearly`,
      n: $localize`latest snapshots`
    };

    return Object.entries(retention)
      .filter(([, interval]) => interval !== null && interval !== undefined)
      .map(([frequency, interval]) => `${interval} ${retentionLabels[frequency] || frequency}`);
  }

  formatScheduleDate(value?: string | Date | null): string {
    if (!value) {
      return '-';
    }

    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '-' : date.toLocaleString();
  }

  private calculateNextSync(policy: MirrorPathSchedule): string {
    if (!policy?.schedule) {
      return '-';
    }

    const baseTime = policy.last ?? policy.start;
    if (!baseTime) {
      return '-';
    }

    const baseDate = new Date(baseTime);
    if (Number.isNaN(baseDate.getTime())) {
      return '-';
    }

    const scheduleMatch = policy.schedule.trim().match(/^(\d+)([a-zA-Z])$/);
    if (!scheduleMatch) {
      return '-';
    }

    const interval = parseInt(scheduleMatch[1], 10);
    const unit = scheduleMatch[2];
    const nextSync = new Date(baseDate);

    switch (unit) {
      case 'm':
        nextSync.setMinutes(nextSync.getMinutes() + interval);
        break;
      case 'h':
        nextSync.setHours(nextSync.getHours() + interval);
        break;
      case 'd':
        nextSync.setDate(nextSync.getDate() + interval);
        break;
      case 'w':
        nextSync.setDate(nextSync.getDate() + interval * 7);
        break;
      case 'M':
        nextSync.setMonth(nextSync.getMonth() + interval);
        break;
      case 'y':
      case 'Y':
        nextSync.setFullYear(nextSync.getFullYear() + interval);
        break;
      default:
        return '-';
    }

    return nextSync.toLocaleString();
  }

  private normalizePath(path?: string): string {
    if (!path) {
      return '';
    }
    return path.replace(/([/](\.\.?)){1,}\s*$/, '').replace(/\/$/, '') || '/';
  }

  private toTitleCase(value: string): string {
    return value ? value.charAt(0).toUpperCase() + value.slice(1) : '-';
  }
}
