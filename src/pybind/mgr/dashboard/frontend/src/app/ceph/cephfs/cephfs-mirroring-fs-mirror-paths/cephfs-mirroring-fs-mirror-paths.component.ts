import {
  Component,
  OnInit,
  OnDestroy,
  TemplateRef,
  ViewChild,
  ViewEncapsulation,
  inject
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { tap } from 'rxjs/operators';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons, ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { MirrorDirStatus, MirrorStatusResponse } from '~/app/shared/models/cephfs.model';
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

  removePathModal(): void {
    const path = this.selection.first().path;
    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
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
    this.mirrorPathsSubscription = this.cephfsService.getMirrorStatus(this.fsName).subscribe(
      (data: MirrorStatusResponse) => {
        this.mirrorPaths = this.parseMirrorStatus(data);
        if (this.selectedPath) {
          this.selectedPath =
            this.mirrorPaths.find((mirrorPath) => mirrorPath.path === this.selectedPath?.path) ??
            null;
          this.sidePanelOpen = !!this.selectedPath;
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
      checkpointCount: peerInfo.snaps_deleted ?? 0,
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
            eta: currentSnap?.eta
          })
        );
      } else if (currentName !== lastName) {
        snapshots.push(
          this.createSnapshotEntry({
            name: currentName,
            status: syncStatus === 'failed' ? 'failed' : 'pending'
          })
        );
      }
    }

    if (lastName && lastName !== '-') {
      snapshots.push(
        this.createSnapshotEntry({
          name: lastName,
          status: 'replicated'
        })
      );
    }

    return snapshots;
  }

  private createSnapshotEntry(entry: {
    name: string;
    status: SnapshotReplicationStatus;
    eta?: string;
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

  onPathClick(path: MirrorPath): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;

    setTimeout(() => {
      this.selectedPath = path;
      this.sidePanelOpen = true;
      this.loadSchedulePolicies(path.path);
      this.loadMirrorPaths();
    });
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
    this.schedulePolicies = [];
    this.schedulePoliciesLoading = false;
    this.removingSchedule = '';
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
