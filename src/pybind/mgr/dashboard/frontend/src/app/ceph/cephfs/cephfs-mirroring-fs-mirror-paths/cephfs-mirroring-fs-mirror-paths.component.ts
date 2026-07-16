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
import { forkJoin, of, Subscription } from 'rxjs';
import { catchError, map, switchMap, tap } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons, ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { MirrorDirStatus, MirrorCheckpoint, MirrorStatusResponse } from '~/app/shared/models/cephfs.model';
import { MirrorPathSchedule } from '~/app/shared/models/snapshot-schedule';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

type SnapshotReplicationStatus = 'in-progress' | 'replicated' | 'pending' | 'failed';
type SyncStatus = 'syncing' | 'idle' | 'failed' | 'completed';

interface SnapshotEntry {
  name: string;
  status: SnapshotReplicationStatus;
  eta?: string;
  icon: keyof typeof ICON_TYPE;
  iconClass: string;
  statusLabel: string;
  filesSynced?: number;
  totalFiles?: number;
  bytesSynced?: number;
  totalBytes?: number;
  createdAt?: string;
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

const SNAPSHOT_STATUS_ICONS: Record<SnapshotReplicationStatus, keyof typeof ICON_TYPE> = {
  'in-progress': 'inProgress',
  replicated: 'checkMarkOutline',
  pending: 'pendingFilled',
  failed: 'danger'
};

const SNAPSHOT_STATUS_CLASSES: Record<SnapshotReplicationStatus, string> = {
  'in-progress': 'info',
  replicated: 'success',
  pending: 'muted',
  failed: 'danger'
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
  private taskWrapper = inject(TaskWrapperService);

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
  snapshotPanels: SnapshotPanelViewModel[] = [];
  pathCheckpoints: MirrorCheckpoint[] = [];
  checkpointsLoading = false;
  checkpointActionInProgress = '';
  expandedSnapshotNames = new Set<string>();

  private subscriptions = new Subscription();
  private mirrorPathsSubscription?: Subscription;

  ngOnInit(): void {
    this.initializeColumns();
    this.initializeTableActions();
    this.fetchFsName();
  }

  ngOnDestroy(): void {
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
        name: $localize`Checkpoints`,
        prop: 'checkpointCount',
        flexGrow: 1.5,
        sortable: true
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
      this.cephfsService
        .list()
        .subscribe((filesystems: { id?: number; mdsmap?: { fs_name?: string } }[]) => {
          const fsId =
            filesystems.find((fs) => fs.mdsmap?.fs_name === this.fsName)?.id ?? 0;
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
        this.taskWrapper
          .wrapTaskAroundCall({
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
    this.mirrorPathsSubscription = this.cephfsService
      .getMirrorStatus(this.fsName)
      .pipe(
        switchMap((data: MirrorStatusResponse) => {
          const paths = this.parseMirrorStatus(data);
          if (!paths.length) {
            return of(paths);
          }

          return forkJoin(
            paths.map((mirrorPath) =>
              this.cephfsService.listMirrorCheckpoints(this.fsName, mirrorPath.path).pipe(
                map((response) => response.checkpoints?.length ?? 0),
                catchError(() => of(0))
              )
            )
          ).pipe(
            map((checkpointCounts) => {
              paths.forEach((mirrorPath, index) => {
                mirrorPath.checkpointCount = checkpointCounts[index];
              });
              return paths;
            })
          );
        })
      )
      .subscribe(
        (mirrorPaths) => {
          this.mirrorPaths = mirrorPaths;
          if (this.selectedPath) {
            this.selectedPath =
              this.mirrorPaths.find((mirrorPath) => mirrorPath.path === this.selectedPath?.path) ??
              null;
            this.sidePanelOpen = !!this.selectedPath;
            if (this.selectedPath) {
              this.refreshSnapshotPanels();
            }
          }
        },
        (_) => {
          this.mirrorPaths = [];
          this.selectedPath = null;
          this.sidePanelOpen = false;
        }
      );
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
        peerInfo.last_synced_snap?.sync_time_stamp != null
          ? String(peerInfo.last_synced_snap.sync_time_stamp)
          : undefined,
      snapshotCount: peerInfo.snaps_synced ?? 0,
      pendingSnapshotCount: snapshots.filter(
        (snapshot) => snapshot.status === 'in-progress' || snapshot.status === 'pending'
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
      if (syncStatus === 'syncing') {
        snapshots.push(
          this.createSnapshotEntry({
            name: currentName,
            status: 'in-progress',
            eta: currentSnap?.eta,
            filesSynced: currentSnap?.files?.sync_files,
            totalFiles: currentSnap?.files?.total_files,
            bytesSynced: this.parseByteValue(currentSnap?.bytes?.sync_bytes),
            totalBytes: this.parseByteValue(currentSnap?.bytes?.total_bytes)
          })
        );
      } else if (currentName !== lastName) {
        snapshots.push(
          this.createSnapshotEntry({
            name: currentName,
            status: syncStatus === 'failed' ? 'failed' : 'pending',
            filesSynced: currentSnap?.files?.sync_files,
            totalFiles: currentSnap?.files?.total_files,
            bytesSynced: this.parseByteValue(currentSnap?.bytes?.sync_bytes),
            totalBytes: this.parseByteValue(currentSnap?.bytes?.total_bytes)
          })
        );
      }
    }

    if (lastName && lastName !== '-') {
      snapshots.push(
        this.createSnapshotEntry({
          name: lastName,
          status: 'replicated',
          filesSynced: lastSnap?.sync_files,
          totalFiles: lastSnap?.sync_files,
          bytesSynced: this.parseByteValue(
            lastSnap?.sync_bytes != null ? String(lastSnap.sync_bytes) : undefined
          ),
          createdAt:
            lastSnap?.sync_time_stamp != null ? String(lastSnap.sync_time_stamp) : undefined
        })
      );
    }

    return snapshots;
  }

  private createSnapshotEntry(entry: {
    name: string;
    status: SnapshotReplicationStatus;
    eta?: string;
    filesSynced?: number;
    totalFiles?: number;
    bytesSynced?: number;
    totalBytes?: number;
    createdAt?: string;
  }): SnapshotEntry {
    return {
      ...entry,
      icon: SNAPSHOT_STATUS_ICONS[entry.status],
      iconClass: SNAPSHOT_STATUS_CLASSES[entry.status],
      statusLabel: this.snapshotStatusLabel(entry.status)
    };
  }

  private snapshotStatusLabel(status: SnapshotReplicationStatus): string {
    switch (status) {
      case 'in-progress':
        return $localize`replication in-progress`;
      case 'replicated':
        return $localize`replicated.`;
      case 'pending':
        return $localize`replication pending`;
      case 'failed':
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
      this.selectedPath?.syncStatus === 'syncing' && this.selectedPath?.syncProgress !== undefined
    );
  }

  get selectedPathCheckpointCount(): number {
    return this.pathCheckpoints.length;
  }

  get canMarkCheckpoint(): boolean {
    return !!this.permission?.create;
  }

  get canRemoveCheckpoint(): boolean {
    return !!this.permission?.delete;
  }

  onPathClick(path: MirrorPath): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
    this.snapshotPanels = [];
    this.pathCheckpoints = [];
    this.expandedSnapshotNames.clear();

    setTimeout(() => {
      this.selectedPath = path;
      this.sidePanelOpen = true;
      this.loadSchedulePolicies(path.path);
      this.loadPathCheckpoints(path.path);
      this.loadMirrorPaths();
    });
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
    this.schedulePolicies = [];
    this.schedulePoliciesLoading = false;
    this.removingSchedule = '';
    this.snapshotPanels = [];
    this.pathCheckpoints = [];
    this.checkpointsLoading = false;
    this.checkpointActionInProgress = '';
    this.expandedSnapshotNames.clear();
  }

  loadPathCheckpoints(path: string): void {
    if (!this.fsName || !path) {
      this.pathCheckpoints = [];
      this.refreshSnapshotPanels();
      return;
    }

    this.checkpointsLoading = true;
    this.subscriptions.add(
      this.cephfsService.listMirrorCheckpoints(this.fsName, path).subscribe(
        (response) => {
          if (this.selectedPath?.path !== path) {
            this.checkpointsLoading = false;
            return;
          }
          this.pathCheckpoints = response.checkpoints ?? [];
          if (this.selectedPath) {
            this.selectedPath.checkpointCount = this.pathCheckpoints.length;
          }
          this.refreshSnapshotPanels();
          this.checkpointsLoading = false;
        },
        () => {
          if (this.selectedPath?.path === path) {
            this.pathCheckpoints = [];
            this.refreshSnapshotPanels();
          }
          this.checkpointsLoading = false;
        }
      )
    );
  }

  refreshSnapshotPanels(): void {
    if (!this.selectedPath) {
      this.snapshotPanels = [];
      return;
    }

    const checkpointByName = new Map(
      this.pathCheckpoints.map((checkpoint) => [checkpoint.snap_name, checkpoint])
    );
    const snapshots = this.selectedPath.snapshots ?? [];
    const seenNames = new Set<string>();

    this.snapshotPanels = snapshots.map((snapshot) => {
      seenNames.add(snapshot.name);
      const checkpoint = checkpointByName.get(snapshot.name);
      return this.buildSnapshotPanel(snapshot, checkpoint);
    });

    for (const checkpoint of this.pathCheckpoints) {
      if (seenNames.has(checkpoint.snap_name)) {
        continue;
      }
      seenNames.add(checkpoint.snap_name);
      this.snapshotPanels.push(
        this.buildSnapshotPanel(
          this.createSnapshotEntry({
            name: checkpoint.snap_name,
            status: 'replicated',
            createdAt: checkpoint.created_at
          }),
          checkpoint
        )
      );
    }
  }

  private buildSnapshotPanel(
    snapshot: SnapshotEntry,
    checkpoint?: MirrorCheckpoint
  ): SnapshotPanelViewModel {
    return {
      ...snapshot,
      expanded: this.expandedSnapshotNames.has(snapshot.name),
      hasCheckpoint: !!checkpoint,
      checkpoint,
      createdAt: checkpoint?.created_at ?? snapshot.createdAt,
      replicationStatusLabel: this.replicationStatusLabel(snapshot.status)
    };
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
    this.checkpointActionInProgress = snapName;
    this.subscriptions.add(
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('cephfs/mirroring/checkpoint/add', {
            fsName: this.fsName,
            path,
            snapName
          }),
          call: this.cephfsService
            .addMirrorCheckpoint(this.fsName, path, snapName)
            .pipe(
              tap(() => {
                this.checkpointActionInProgress = '';
                this.loadPathCheckpoints(path);
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
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('cephfs/mirroring/checkpoint/remove', {
              fsName: this.fsName,
              path,
              snapName
            }),
            call: this.cephfsService.removeMirrorCheckpoint(this.fsName, path, snapName).pipe(
              tap(() => {
                this.loadPathCheckpoints(path);
                this.loadMirrorPaths();
              })
            )
          })
    });
  }

  formatSnapshotFiles(filesSynced?: number, totalFiles?: number): string {
    if (filesSynced === undefined || totalFiles === undefined) {
      return '-';
    }
    return `${filesSynced}/${totalFiles}`;
  }

  private replicationStatusLabel(status: SnapshotReplicationStatus): string {
    switch (status) {
      case 'in-progress':
        return $localize`In progress`;
      case 'replicated':
        return $localize`Replicated`;
      case 'pending':
        return $localize`Pending`;
      case 'failed':
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
