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
import { Icons } from '~/app/shared/enum/icons.enum';

interface MirrorPath {
  path: string;
  syncStatus: 'syncing' | 'idle' | 'failed';
  currentSyncSnapshot: string;
  currentSyncEta?: string;
  lastSyncedSnapshot: string;
  lastSyncedTime?: string;
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
  syncStatusTpl!: TemplateRef<any>;

  @ViewChild('pathTpl', { static: true })
  pathTpl!: TemplateRef<any>;

  @ViewChild('currentSyncSnapshotTpl', { static: true })
  currentSyncSnapshotTpl!: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  mirrorPaths: MirrorPath[] = [];
  selection = new CdTableSelection();
  selectedPath: MirrorPath | null = null;
  sidePanelOpen = false;
  icons = Icons;
  fsName: string = '';

  private subscriptions = new Subscription();

  constructor(private cephfsService: CephfsService, private route: ActivatedRoute) {}

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
        (data: any) => {
          this.mirrorPaths = this.parseMirrorStatus(data);
        },
        (_) => {
          this.mirrorPaths = [];
        }
      )
    );
  }

  parseMirrorStatus(data: any): MirrorPath[] {
    const paths: MirrorPath[] = [];

    if (!data || !data.metrics) {
      return paths;
    }

    for (const [path, pathData] of Object.entries(data.metrics)) {
      if (pathData && typeof pathData === 'object' && 'peer' in pathData) {
        const peerData: any = pathData;

        const peerEntries = Object.entries(peerData.peer || {});
        if (peerEntries.length > 0) {
          const [, peerInfo]: [string, any] = peerEntries[0];

          const syncStatus = peerInfo.state || 'idle';
          const lastSyncedSnap = peerInfo.last_synced_snap?.name || '-';
          const currentSyncSnap = peerInfo.current_sync_snap?.name || '-';
          let currentSyncEta: string | undefined;
          if (peerInfo.current_sync_snap?.eta_completion) {
            currentSyncEta = peerInfo.current_sync_snap.eta_completion;
          }

          const mirrorPath: MirrorPath = {
            path: path,
            syncStatus: syncStatus,
            currentSyncSnapshot: currentSyncSnap,
            currentSyncEta: currentSyncEta,
            lastSyncedSnapshot: lastSyncedSnap
          };

          paths.push(mirrorPath);
        }
      }
    }

    return paths;
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
