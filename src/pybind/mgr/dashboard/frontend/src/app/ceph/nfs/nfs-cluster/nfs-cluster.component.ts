import { Component, OnInit, inject } from '@angular/core';
import { BehaviorSubject, Observable, forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

import { NFSBackend, NFSCluster } from '../models/nfs-cluster-config';

interface NFSClusterRow extends NFSCluster {
  status: string;
  shares: number;
  protocol: string;
}

@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  standalone: false
})
export class NfsClusterComponent implements OnInit {
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;
  orchStatus: OrchestratorStatus;
  clusters$: Observable<NFSClusterRow[]>;
  subject = new BehaviorSubject<NFSClusterRow[]>([]);

  private authStorageService = inject(AuthStorageService);
  private nfsService = inject(NfsService);
  private orchService = inject(OrchestratorService);

  constructor() {
    this.permission = this.authStorageService.getPermissions().nfs;
  }

  ngOnInit(): void {
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
    this.permission = this.authStorageService.getPermissions().nfs;
    this.clusters$ = this.subject.pipe(
      switchMap(() =>
        forkJoin({
          clusters: this.nfsService.nfsClusterList(),
          exports: this.nfsService.list().pipe(catchError(() => of([])))
        }).pipe(
          map(({ clusters, exports }) => this.mapClusterRows(clusters, exports as any[])),
          catchError(() => of([]))
        )
      )
    );
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 1,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            Running: { class: 'tag-success' },
            Degraded: { class: 'tag-warning' },
            Stopped: { class: 'tag-danger' },
            Unknown: { class: 'tag-default' }
          }
        }
      },
      {
        name: $localize`Shares`,
        prop: 'shares',
        flexGrow: 1
      },
      {
        name: $localize`Protocol`,
        prop: 'protocol',
        flexGrow: 1
      }
    ];
  }

  private mapClusterRows(clusters: NFSCluster[], exports: any[]): NFSClusterRow[] {
    const shareCounts = (exports || []).reduce((acc: Record<string, number>, exp: any) => {
      const clusterId = exp?.cluster_id;
      if (clusterId) {
        acc[clusterId] = (acc[clusterId] || 0) + 1;
      }
      return acc;
    }, {});

    return (clusters || []).map((cluster) => ({
      ...cluster,
      status: this.getClusterStatus(cluster.backend),
      shares: shareCounts[cluster.name] || 0,
      protocol: cluster.enable_nfsv3 ? $localize`NFSv3, NFSv4` : $localize`NFSv4`
    }));
  }

  private getClusterStatus(backends: NFSBackend[] = []): string {
    if (!backends?.length) {
      return $localize`Unknown`;
    }
    const statuses = backends.map((backend) => (backend.status || '').toLowerCase());
    const running = statuses.filter((status) => status === 'running').length;
    if (running === statuses.length) {
      return $localize`Running`;
    }
    if (running > 0) {
      return $localize`Degraded`;
    }
    return $localize`Stopped`;
  }

  loadData() {
    this.subject.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
