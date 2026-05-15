import { Component, OnInit, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';

interface Checkpoint {
  id: string;
  status: 'completed' | 'pending' | 'failed';
  timestamp: string;
  backlog?: number;
}

interface SchedulePolicy {
  name: string;
  schedule: string;
  enabled: boolean;
  lastRun: string;
  nextRun: string;
}

interface MirrorPath {
  path: string;
  syncStatus: 'syncing' | 'completed' | 'failed' | 'idle';
  filesSynced: number;
  totalFiles: number;
  bytesSynced: number;
  totalBytes: number;
  currentSyncSnapshot: string;
  lastSyncSnapshot: string;
  priorityMode: string;
  eta: string;
  lastSyncTime: string;
  syncProgress: number;
  snapshotsSynced: number;
  snapshotsDeleted: number;
  snapshotsRenamed: number;
  checkpoints: Checkpoint[];
  schedulePolicy: SchedulePolicy;
  failureReason?: string;
}

@Component({
  selector: 'cd-cephfs-mirroring-fs-mirror-paths',
  templateUrl: './cephfs-mirroring-fs-mirror-paths.component.html',
  styleUrls: ['./cephfs-mirroring-fs-mirror-paths.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringFsMirrorPathsComponent implements OnInit {
  @ViewChild('syncStatusTpl', { static: true })
  syncStatusTpl!: TemplateRef<any>;

  @ViewChild('filesSyncedTpl', { static: true })
  filesSyncedTpl!: TemplateRef<any>;

  @ViewChild('bytesSyncedTpl', { static: true })
  bytesSyncedTpl!: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  mirrorPaths: MirrorPath[] = [];
  selection = new CdTableSelection();
  selectedPath: MirrorPath | null = null;
  sidePanelOpen = false;
  activeTab: 'details' | 'checkpoint' | 'schedule' = 'details';
  icons = Icons;

  ngOnInit(): void {
    this.initializeColumns();
    this.loadMockData();
  }

  initializeColumns(): void {
    this.columns = [
      {
        name: $localize`Path`,
        prop: 'path',
        flexGrow: 2,
        sortable: true
      },
      {
        name: $localize`Sync Status`,
        prop: 'syncStatus',
        flexGrow: 1.5,
        cellTemplate: this.syncStatusTpl,
        sortable: true
      },
      {
        name: $localize`Files Synced`,
        prop: 'filesSynced',
        flexGrow: 1.5,
        cellTemplate: this.filesSyncedTpl,
        sortable: true
      },
      {
        name: $localize`Bytes Synced`,
        prop: 'bytesSynced',
        flexGrow: 1.5,
        cellTemplate: this.bytesSyncedTpl,
        sortable: true
      },
      {
        name: $localize`Current Sync Snapshot`,
        prop: 'currentSyncSnapshot',
        flexGrow: 1.5,
        sortable: true
      },
      {
        name: $localize`Last Sync Snapshot`,
        prop: 'lastSyncSnapshot',
        flexGrow: 1.5,
        sortable: true
      }
    ];
  }

  loadMockData(): void {
    // Mock data representing realistic CephFS mirror paths
    this.mirrorPaths = [
      {
        path: '/data/production',
        syncStatus: 'syncing',
        filesSynced: 650,
        totalFiles: 1000,
        bytesSynced: 1288490188.8, // 1.2 GB
        totalBytes: 2147483648, // 2.0 GB
        currentSyncSnapshot: 'snap1234',
        lastSyncSnapshot: 'snap123',
        priorityMode: 'Per Thread',
        eta: '5 min',
        lastSyncTime: '1:37 PM',
        syncProgress: 60,
        snapshotsSynced: 20,
        snapshotsDeleted: 12,
        snapshotsRenamed: 4,
        checkpoints: [
          {
            id: 'checkpoint-001',
            status: 'completed',
            timestamp: '2026-06-16T08:30:00Z',
            backlog: 0
          },
          {
            id: 'checkpoint-002',
            status: 'pending',
            timestamp: '2026-06-16T09:00:00Z',
            backlog: 5
          }
        ],
        schedulePolicy: {
          name: 'Hourly Sync',
          schedule: '0 * * * *',
          enabled: true,
          lastRun: '2026-06-16T08:00:00Z',
          nextRun: '2026-06-16T10:00:00Z'
        }
      },
      {
        path: '/data/backup',
        syncStatus: 'completed',
        filesSynced: 2000,
        totalFiles: 2000,
        bytesSynced: 5368709120, // 5.0 GB
        totalBytes: 5368709120, // 5.0 GB
        currentSyncSnapshot: 'snap5678',
        lastSyncSnapshot: 'snap5678',
        priorityMode: 'Thread Shared',
        eta: '0 min',
        lastSyncTime: '12:45 PM',
        syncProgress: 100,
        snapshotsSynced: 35,
        snapshotsDeleted: 8,
        snapshotsRenamed: 2,
        checkpoints: [
          {
            id: 'checkpoint-003',
            status: 'completed',
            timestamp: '2026-06-16T07:45:00Z',
            backlog: 0
          },
          {
            id: 'checkpoint-004',
            status: 'completed',
            timestamp: '2026-06-16T08:45:00Z',
            backlog: 0
          }
        ],
        schedulePolicy: {
          name: 'Daily Backup',
          schedule: '0 0 * * *',
          enabled: true,
          lastRun: '2026-06-16T00:00:00Z',
          nextRun: '2026-06-17T00:00:00Z'
        }
      },
      {
        path: '/data/archive',
        syncStatus: 'failed',
        filesSynced: 150,
        totalFiles: 500,
        bytesSynced: 536870912, // 0.5 GB
        totalBytes: 2684354560, // 2.5 GB
        currentSyncSnapshot: 'snap9012',
        lastSyncSnapshot: 'snap8901',
        priorityMode: 'Per Thread',
        eta: 'N/A',
        lastSyncTime: '11:20 AM',
        syncProgress: 30,
        snapshotsSynced: 8,
        snapshotsDeleted: 3,
        snapshotsRenamed: 1,
        checkpoints: [
          {
            id: 'checkpoint-005',
            status: 'completed',
            timestamp: '2026-06-16T06:00:00Z',
            backlog: 0
          },
          {
            id: 'checkpoint-006',
            status: 'failed',
            timestamp: '2026-06-16T07:20:00Z',
            backlog: 15
          }
        ],
        schedulePolicy: {
          name: 'Weekly Archive',
          schedule: '0 0 * * 0',
          enabled: true,
          lastRun: '2026-06-15T00:00:00Z',
          nextRun: '2026-06-22T00:00:00Z'
        },
        failureReason: 'Connection timeout to remote cluster'
      },
      {
        path: '/data/logs',
        syncStatus: 'idle',
        filesSynced: 3500,
        totalFiles: 3500,
        bytesSynced: 1073741824, // 1.0 GB
        totalBytes: 1073741824, // 1.0 GB
        currentSyncSnapshot: 'snap3456',
        lastSyncSnapshot: 'snap3456',
        priorityMode: 'Thread Shared',
        eta: 'N/A',
        lastSyncTime: '10:15 AM',
        syncProgress: 100,
        snapshotsSynced: 42,
        snapshotsDeleted: 18,
        snapshotsRenamed: 6,
        checkpoints: [
          {
            id: 'checkpoint-007',
            status: 'completed',
            timestamp: '2026-06-16T05:15:00Z',
            backlog: 0
          }
        ],
        schedulePolicy: {
          name: 'Every 6 Hours',
          schedule: '0 */6 * * *',
          enabled: false,
          lastRun: '2026-06-16T06:00:00Z',
          nextRun: 'N/A'
        }
      },
      {
        path: '/data/media',
        syncStatus: 'syncing',
        filesSynced: 840,
        totalFiles: 2000,
        bytesSynced: 3435973836.8, // 3.2 GB
        totalBytes: 8053063680, // 7.5 GB
        currentSyncSnapshot: 'snap7890',
        lastSyncSnapshot: 'snap7889',
        priorityMode: 'Per Thread',
        eta: '12 min',
        lastSyncTime: '1:15 PM',
        syncProgress: 42,
        snapshotsSynced: 15,
        snapshotsDeleted: 5,
        snapshotsRenamed: 3,
        checkpoints: [
          {
            id: 'checkpoint-008',
            status: 'completed',
            timestamp: '2026-06-16T08:15:00Z',
            backlog: 0
          },
          {
            id: 'checkpoint-009',
            status: 'pending',
            timestamp: '2026-06-16T09:15:00Z',
            backlog: 8
          }
        ],
        schedulePolicy: {
          name: 'Continuous Sync',
          schedule: '*/15 * * * *',
          enabled: true,
          lastRun: '2026-06-16T09:00:00Z',
          nextRun: '2026-06-16T09:15:00Z'
        }
      }
    ];
  }

  onSelectionChange(selection: CdTableSelection): void {
    this.selection = selection;
    if (selection.hasSelection && selection.first()) {
      this.selectedPath = selection.first();
      this.sidePanelOpen = true;
      this.activeTab = 'details';
    }
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
    this.selectedPath = null;
  }

  setActiveTab(tab: 'details' | 'checkpoint' | 'schedule'): void {
    this.activeTab = tab;
  }

  getSyncStatusIcon(status: string): string {
    switch (status) {
      case 'syncing':
        return Icons.inProgress;
      case 'completed':
        return Icons.success;
      case 'failed':
        return Icons.danger;
      case 'idle':
        return Icons.circleDash;
      default:
        return Icons.circle;
    }
  }

  getSyncStatusClass(status: string): string {
    switch (status) {
      case 'syncing':
        return 'status-syncing';
      case 'completed':
        return 'status-completed';
      case 'failed':
        return 'status-failed';
      case 'idle':
        return 'status-idle';
      default:
        return '';
    }
  }

  getCheckpointStatusIcon(status: string): string {
    switch (status) {
      case 'completed':
        return Icons.success;
      case 'pending':
        return Icons.inProgress;
      case 'failed':
        return Icons.danger;
      default:
        return Icons.circle;
    }
  }

  formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  formatTimestamp(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  }

  formatSchedule(schedule: string): string {
    // Convert cron expression to human-readable format
    if (schedule === '0 * * * *') return 'Every hour';
    if (schedule === '0 0 * * *') return 'Daily at midnight';
    if (schedule === '0 0 * * 0') return 'Weekly on Sunday';
    if (schedule === '0 */6 * * *') return 'Every 6 hours';
    if (schedule === '*/15 * * * *') return 'Every 15 minutes';
    return schedule;
  }
}

// Made with Bob
