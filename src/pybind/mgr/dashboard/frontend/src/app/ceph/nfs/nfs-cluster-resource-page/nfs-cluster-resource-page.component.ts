import { Component, DestroyRef, inject, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import {
  getClusterStatus,
  NFSCluster,
  NFSClusterPlacement,
  NFSHostPattern
} from '../models/nfs-cluster-config';

@Component({
  selector: 'cd-nfs-cluster-resource-page',
  templateUrl: './nfs-cluster-resource-page.component.html',
  styleUrls: ['./nfs-cluster-resource-page.component.scss'],
  standalone: false
})
export class NfsClusterResourcePageComponent implements OnInit {
  private destroyRef = inject(DestroyRef);

  section = '';
  clusterId = '';
  selection: NFSCluster | undefined;
  loadError = false;
  isOverviewLoading = false;
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
        this.isOverviewLoading = !!this.clusterId;
        this.loadError = false;
      });

    this.nfsClusterResourceStateService.cluster$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((cluster: NFSCluster | null) => {
        this.applyCluster(cluster);
      });
  }

  private applyCluster(cluster: NFSCluster | null): void {
    this.isOverviewLoading = false;
    if (!this.clusterId) {
      this.selection = undefined;
      this.loadError = false;
      this.overviewFields = [];
      return;
    }

    this.selection = cluster || undefined;
    this.loadError = !cluster;
    this.overviewFields = cluster ? this.buildOverviewFields(cluster) : [];
  }

  private buildOverviewFields(cluster: NFSCluster): OverviewField[] {
    const clusterStatus = getClusterStatus(cluster.backend);
    const haEnabled =
      !!cluster.ingress_mode ||
      ['active-active', 'active-passive'].includes(cluster.deployment_type || '');

    const statusMap: Record<string, 'success' | 'warning' | 'danger' | 'info-circle'> = {
      Active: 'success',
      Degraded: 'warning',
      Stopped: 'danger',
      Unknown: 'info-circle'
    };

    return [
      { label: $localize`Name`, value: cluster.name },
      {
        label: $localize`Status`,
        value: clusterStatus,
        type: 'status',
        status: statusMap[clusterStatus]
      },
      { label: $localize`Protocol`, value: this.formatProtocol(cluster) },
      {
        label: $localize`Service port`,
        value: cluster.port ?? cluster.backend?.find((backend) => backend.port != null)?.port ?? '-'
      },
      {
        label: $localize`High availability`,
        value: haEnabled ? $localize`Enabled` : $localize`Disabled`
      },
      { label: $localize`Monitoring port`, value: cluster.monitor_port ?? '-' },
      {
        label: $localize`Service instances`,
        value: cluster.placement?.count ?? cluster.backend?.length ?? 0
      },
      { label: $localize`Placement`, value: this.formatPlacement(cluster.placement) }
    ];
  }

  private formatProtocol(cluster: NFSCluster): string {
    return cluster.enable_nfsv3 ? $localize`NFSv3, NFSv4` : $localize`NFSv4`;
  }

  private formatPlacement(placement?: NFSClusterPlacement): string {
    if (placement?.label) {
      return $localize`Label: ${placement.label}`;
    }
    if (placement?.hosts?.length) {
      return placement.hosts.join(', ');
    }
    const hostPattern = this.formatHostPattern(placement?.host_pattern);
    if (hostPattern) {
      return hostPattern;
    }
    return $localize`Automatic`;
  }

  private formatHostPattern(hostPattern?: NFSHostPattern): string | null {
    if (!hostPattern) {
      return null;
    }
    if (typeof hostPattern === 'string') {
      return hostPattern;
    }
    return hostPattern.pattern || null;
  }
}
