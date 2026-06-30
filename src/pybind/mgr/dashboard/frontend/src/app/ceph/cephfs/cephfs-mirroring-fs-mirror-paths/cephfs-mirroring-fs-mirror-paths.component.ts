import {
  Component,
  OnInit,
  OnDestroy,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { MirrorDirStatus, MirrorStatusResponse } from '~/app/shared/models/cephfs.model';

interface MirrorPath {
  path: string;
  syncStatus: 'syncing' | 'idle' | 'failed';
  currentSyncSnapshot: string;
  currentSyncEta?: string;
  currentSyncMode?: string;
  lastSyncedSnapshot: string;
  lastSyncedTime?: string;
  snapshotCount?: number;
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

  columns: CdTableColumn[] = [];
  mirrorPaths: MirrorPath[] = [];
  selection = new CdTableSelection();
  selectedPath: MirrorPath | null = null;
  sidePanelOpen = false;
  fsName: string = '';

  private subscriptions = new Subscription();

  constructor(
    private cephfsService: CephfsService,
    private route: ActivatedRoute,
    private formatterService: FormatterService
  ) {}

  ngOnInit(): void {
    this.initializeColumns();
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

    this.cephfsService.getMirrorStatus(this.fsName).subscribe(
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

    return {
      path,
      syncStatus: (peerInfo.state ?? 'idle') as 'syncing' | 'idle' | 'failed',
      currentSyncSnapshot: currentSnap?.name ?? '-',
      currentSyncEta: currentSnap?.eta,
      currentSyncMode: currentSnap?.['sync-mode'],
      lastSyncedSnapshot: peerInfo.last_synced_snap?.name ?? '-',
      lastSyncedTime:
        peerInfo.last_synced_snap?.sync_time_stamp != null
          ? String(peerInfo.last_synced_snap.sync_time_stamp)
          : undefined,
      snapshotCount: peerInfo.snaps_synced ?? 0,
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

  onPathClick(path: MirrorPath): void {
    this.selectedPath = path;
    this.sidePanelOpen = true;
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
  }

  getSyncStatusIcon(status: string): keyof typeof ICON_TYPE {
    switch (status) {
      case 'syncing':
        return 'inProgress';
      case 'idle':
        return 'pendingFilled';
      case 'failed':
        return 'danger';
      case 'completed':
        return 'checkMarkOutline';
      default:
        return 'infoCircle';
    }
  }

  getSyncStatusClass(status: string): string {
    switch (status) {
      case 'syncing':
        return 'info';
      case 'completed':
        return 'success';
      case 'idle':
        return 'muted';
      case 'failed':
        return 'danger';
      default:
        return '';
    }
  }
}
