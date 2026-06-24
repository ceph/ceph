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
import { CephfsService, SnapshotMirrorStatusResponse } from '~/app/shared/api/cephfs.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { FormatterService } from '~/app/shared/services/formatter.service';

interface MirrorPath {
  path: string;
  syncStatus: 'syncing' | 'idle' | 'failed';
  currentSyncSnapshot: string;
  currentSyncEta?: string;
  lastSyncedSnapshot: string;
  lastSyncedTime?: string;
  snapshotCount?: number;
  checkpointCount?: number;
  syncProgress?: number;
  filesSynced?: number;
  totalFiles?: number;
  bytesSynced?: number;
  totalBytes?: number;
}

interface PathData {
  peer: Record<string, PeerInfo>;
}

interface PeerInfo {
  state: string;
  current_sync_snap?: CurrentSyncSnap;
  last_synced_snap?: { name: string };
  snaps_synced?: number;
  snaps_deleted?: number;
}

interface CurrentSyncSnap {
  name: string;
  files?: string | number;
  bytes?: string | number;
  eta_completion?: string;
  files_synced?: number;
  total_files?: number;
  bytes_synced?: number;
  total_bytes?: number;
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
  icons = Icons;
  fsName: string = '';

  private subscriptions = new Subscription();

  constructor(
    private cephfsService: CephfsService,
    private route: ActivatedRoute,
    private formatterService: FormatterService
  ) {}

  ngOnInit(): void {
    this.initializeColumns();
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

  fetchFsName(): void {
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

    this.subscriptions.add(
      this.cephfsService.getSnapshotMirrorStatus(this.fsName).subscribe(
        (data: SnapshotMirrorStatusResponse) => {
          this.mirrorPaths = this.parseMirrorStatus(data);
        },
        (_) => {
          this.mirrorPaths = [];
        }
      )
    );
  }

  parseMirrorStatus(data: SnapshotMirrorStatusResponse): MirrorPath[] {
    if (!data?.metrics) {
      return [];
    }

    const paths: MirrorPath[] = [];

    for (const path in data.metrics) {
      const pathData = data.metrics[path] as PathData;
      
      // Skip invalid entries
      if (!pathData?.peer) continue;
      
      const peerInfo = this.extractPeerInfo(pathData);
      if (!peerInfo) continue;
      
      paths.push(this.buildMirrorPath(path, peerInfo));
    }

    return paths;
  }

  private parseFileProgress(files: string | number | undefined): {
    synced: number;
    total: number;
    progress: number;
  } {
    if (!files || typeof files !== 'string') {
      return { synced: 0, total: 0, progress: 0 };
    }
    
    const filesMatch = files.match(/(\d+)\/(\d+)/);
    const percentMatch = files.match(/\((\d+)%\)/);
    
    return {
      synced: filesMatch ? parseInt(filesMatch[1], 10) : 0,
      total: filesMatch ? parseInt(filesMatch[2], 10) : 0,
      progress: percentMatch ? parseInt(percentMatch[1], 10) : 0
    };
  }

  private parseByteProgress(bytes: string | number | undefined): {
    synced: number;
    total: number;
  } {
    if (!bytes || typeof bytes !== 'string') {
      return { synced: 0, total: 0 };
    }
    
    const match = bytes.match(/([\d.]+)\s*(\w+)\/([\d.]+)\s*(\w+)/);
    if (!match) {
      return { synced: 0, total: 0 };
    }
    
    return {
      synced: this.formatterService.toBytes(`${match[1]}${match[2]}`, 0) ?? 0,
      total: this.formatterService.toBytes(`${match[3]}${match[4]}`, 0) ?? 0
    };
  }

  private calculateSyncProgress(
    fileProgress: number,
    filesSynced: number,
    totalFiles: number,
    bytesSynced: number,
    totalBytes: number
  ): number {
    if (fileProgress > 0) return fileProgress;
    if (totalFiles > 0) return Math.round((filesSynced / totalFiles) * 100);
    if (totalBytes > 0) return Math.round((bytesSynced / totalBytes) * 100);
    return 0;
  }

  private extractPeerInfo(pathData: PathData): PeerInfo | null {
    const peerEntries = Object.entries(pathData.peer ?? {});
    return peerEntries.length > 0 ? peerEntries[0][1] : null;
  }

  private buildMirrorPath(path: string, peerInfo: PeerInfo): MirrorPath {
    const currentSnap = peerInfo.current_sync_snap;
    
    // Parse file progress
    const fileProgress = this.parseFileProgress(currentSnap?.files);
    const filesSynced = currentSnap?.files_synced ?? fileProgress.synced;
    const totalFiles = currentSnap?.total_files ?? fileProgress.total;
    
    // Parse byte progress
    const byteProgress = this.parseByteProgress(currentSnap?.bytes);
    const bytesSynced = currentSnap?.bytes_synced ?? byteProgress.synced;
    const totalBytes = currentSnap?.total_bytes ?? byteProgress.total;
    
    // Calculate progress
    const syncProgress = this.calculateSyncProgress(
      fileProgress.progress,
      filesSynced,
      totalFiles,
      bytesSynced,
      totalBytes
    );
    
    return {
      path,
      syncStatus: (peerInfo.state ?? 'idle') as 'syncing' | 'idle' | 'failed',
      currentSyncSnapshot: currentSnap?.name ?? '-',
      currentSyncEta: currentSnap?.eta_completion,
      lastSyncedSnapshot: peerInfo.last_synced_snap?.name ?? '-',
      snapshotCount: peerInfo.snaps_synced ?? 0,
      checkpointCount: peerInfo.snaps_deleted ?? 0,
      syncProgress,
      filesSynced,
      totalFiles,
      bytesSynced,
      totalBytes
    };
  }

  onSelectionChange(selection: CdTableSelection): void {
    this.selection = selection;
  }

  onPathClick(path: MirrorPath): void {
    this.selectedPath = path;
    this.sidePanelOpen = true;
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
  }

  getSyncStatusIcon(status: string): string {
    switch (status) {
      case 'syncing':
        return Icons.inProgress;
      case 'idle':
        return Icons.pendingFilled;
      case 'failed':
        return Icons.danger;
      default:
        return Icons.circle;
    }
  }

  getSyncStatusClass(status: string): string {
    switch (status) {
      case 'syncing':
        return 'text-info';
      case 'completed':
        return 'text-success';
      case 'idle':
        return 'text-muted';
      case 'failed':
        return 'text-danger';
      default:
        return '';
    }
  }
}
