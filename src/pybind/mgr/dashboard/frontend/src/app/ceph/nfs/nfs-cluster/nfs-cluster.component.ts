import { Component, DestroyRef, OnInit, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
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

import { NFSBackend, NFSCluster, NFSShare } from '../models/nfs.model';

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

  private destroyRef = inject(DestroyRef);
  private authStorageService = inject(AuthStorageService);
  private nfsService = inject(NfsService);
  private orchService = inject(OrchestratorService);

  constructor() {
    this.permission = this.authStorageService.getPermissions().nfs;
  }

  ngOnInit(): void {
    this.orchService
      .status()
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((status: OrchestratorStatus) => {
        this.orchStatus = status;
      });
    this.clusters$ = this.subject.pipe(
      switchMap(() =>
        forkJoin({
          clusters: this.nfsService.listClusters().pipe(catchError(() => of([]))),
          exports: this.nfsService.list().pipe(catchError(() => of([])))
        }).pipe(
          map(({ clusters, exports }) => this.mapClusterRows(clusters, exports as NFSShare[]))
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

  private mapClusterRows(clusters: NFSCluster[], exports: NFSShare[]): NFSClusterRow[] {
    const shareCounts = (exports || []).reduce((acc: Record<string, number>, exp: NFSShare) => {
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
      return '';
    }
    const statuses = backends.map((backend) => (backend.status || '').toLowerCase());
    const running = statuses.filter((status) => status === 'running').length;
    if (running === statuses.length) {
      return 'Running';
    }
    if (running > 0) {
      return 'Degraded';
    }
    return '';
  }

  loadData() {
    this.subject.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
