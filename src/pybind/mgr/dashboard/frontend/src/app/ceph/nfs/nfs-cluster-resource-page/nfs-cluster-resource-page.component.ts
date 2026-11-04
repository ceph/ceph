import { Component, DestroyRef, inject, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { PlacementPipe } from '~/app/ceph/cluster/services/placement.pipe';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import {
  getClusterStatus,
  NFS_CLUSTER_STATUS_MAP,
  NFSCluster,
  NFSClusterPlacement
} from '../models/nfs-cluster-config';

@Component({
  selector: 'cd-nfs-cluster-resource-page',
  templateUrl: './nfs-cluster-resource-page.component.html',
  styleUrls: ['./nfs-cluster-resource-page.component.scss'],
  standalone: false
})
export class NfsClusterResourcePageComponent implements OnInit {
  private destroyRef = inject(DestroyRef);
  private placementPipe = inject(PlacementPipe);

  section = '';
  clusterId = '';
  selection: NFSCluster | undefined;
  loadError = false;
  overviewFields: OverviewField[] = [];

  constructor(
    private route: ActivatedRoute,
    private nfsClusterResourceStateService: NfsClusterResourceStateService
  ) {}

  ngOnInit(): void {
    this.route.data.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((data) => {
      this.section = data['section'] ?? 'overview';
    });

    this.route
      .parent!.paramMap.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((pm: ParamMap) => {
        this.clusterId = pm.get('cluster_id') ?? '';
        this.loadError = false;
      });

    this.nfsClusterResourceStateService.cluster$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((cluster: NFSCluster | null) => {
        this.applyCluster(cluster);
      });
  }

  private applyCluster(cluster: NFSCluster | null): void {
    if (!this.clusterId) {
      this.selection = undefined;
      this.loadError = false;
      this.overviewFields = [];
      return;
    }

    this.selection = cluster || undefined;
    this.loadError = !cluster;
    this.overviewFields = cluster && !cluster.error ? this.buildOverviewFields(cluster) : [];
  }

  private buildOverviewFields(cluster: NFSCluster): OverviewField[] {
    const clusterStatus = getClusterStatus(cluster.backend);
    const haEnabled =
      !!cluster.ingress_mode ||
      ['active-active', 'active-passive'].includes(cluster.deployment_type || '');

    return [
      { label: $localize`Name`, value: cluster.name },
      {
        label: $localize`Status`,
        value: clusterStatus,
        type: 'status',
        status: NFS_CLUSTER_STATUS_MAP[clusterStatus].status
      },
      { label: $localize`Protocol`, value: this.formatProtocol(cluster) },
      {
        label: $localize`Service port`,
        value: cluster.port ?? cluster.backend?.find((backend) => backend.port != null)?.port ?? '-'
      },
      {
        label: $localize`High availability`,
        value: haEnabled ? $localize`Enabled` : $localize`Disabled`,
        type: 'status',
        status: haEnabled ? 'success' : 'danger'
      },
      {
        label: $localize`Monitoring port`,
        value:
          cluster.monitoring_port ??
          cluster.backend?.find((backend) => backend.monitoring_port != null)?.monitoring_port ??
          '-'
      },
      ...(cluster.enable_rdma
        ? [{ label: $localize`RDMA port`, value: cluster.rdma_port ?? '-' }]
        : []),
      {
        label: $localize`Service instances`,
        value: this.serviceInstancesSummary(cluster)
      },
      { label: $localize`Placement`, value: this.formatPlacement(cluster.placement) }
    ];
  }

  private serviceInstancesSummary(cluster: NFSCluster): string {
    const total =
      cluster.placement?.count ?? cluster.placement?.hosts?.length ?? cluster.backend?.length ?? 0;
    const running = (cluster.backend ?? []).filter(
      (b) => b.status?.toLowerCase() === 'running'
    ).length;
    return total ? `${running} / ${total}` : '-';
  }

  private formatProtocol(cluster: NFSCluster): string {
    return cluster.enable_nfsv3 ? $localize`NFSv3, NFSv4` : $localize`NFSv4`;
  }

  private formatPlacement(placement?: NFSClusterPlacement): string {
    if (!placement || !Object.keys(placement).length) {
      return '-';
    }
    const display = this.placementPipe.transform({ placement });
    return display || '-';
  }
}
