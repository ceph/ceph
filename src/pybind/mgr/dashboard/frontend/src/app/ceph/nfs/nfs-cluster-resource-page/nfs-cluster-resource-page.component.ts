import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NFSCluster, NFSClusterPlacement, NFSHostPattern } from '../models/nfs-cluster-config';

@Component({
  selector: 'cd-nfs-cluster-resource-page',
  templateUrl: './nfs-cluster-resource-page.component.html',
  styleUrls: ['./nfs-cluster-resource-page.component.scss'],
  standalone: false
})
export class NfsClusterResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

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
    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? 'overview';
      })
    );

    this.sub.add(
      this.route.parent!.paramMap.subscribe((pm: ParamMap) => {
        this.clusterId = pm.get('cluster_id') ?? '';
        this.isOverviewLoading = !!this.clusterId;
        this.loadError = false;
      })
    );

    this.sub.add(
      this.nfsClusterResourceStateService.cluster$.subscribe((cluster: NFSCluster | null) => {
        this.applyCluster(cluster);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
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
    const isActive = (cluster.backend || []).some((b) => !b.status || b.status === 'running');
    const haEnabled =
      !!cluster.ingress_mode ||
      ['active-active', 'active-passive'].includes(cluster.deployment_type || '');

    return [
      { label: $localize`Name`, value: cluster.name },
      {
        label: $localize`Status`,
        value: isActive ? $localize`Active` : $localize`Inactive`,
        type: 'status',
        status: isActive ? 'success' : 'warning'
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
