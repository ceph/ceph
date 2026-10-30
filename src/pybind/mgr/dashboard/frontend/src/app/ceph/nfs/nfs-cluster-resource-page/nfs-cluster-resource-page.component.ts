import { Component, DestroyRef, inject, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { forkJoin } from 'rxjs';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import {
  NfsClusterResourceStateService,
  NfsClusterState
} from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NfsRateLimitComponent } from '../nfs-rate-limit/nfs-rate-limit.component';
import {
  getClusterStatus,
  NFSBwIopConfig,
  NFSCluster,
  NFSClusterPlacement,
  NFSHostPattern,
  QOSType
} from '../models/nfs-cluster-config';

@Component({
  selector: 'cd-nfs-cluster-resource-page',
  templateUrl: './nfs-cluster-resource-page.component.html',
  styleUrls: ['./nfs-cluster-resource-page.component.scss'],
  standalone: false
})
export class NfsClusterResourcePageComponent implements OnInit {
  @ViewChild(NfsRateLimitComponent) nfsRateLimitComponent!: NfsRateLimitComponent;

  private destroyRef = inject(DestroyRef);

  section = '';
  clusterId = '';
  selection: NFSCluster | undefined;
  qos: NFSBwIopConfig | null = null;
  loadError = false;
  overviewFields: OverviewField[] = [];
  isPerformanceLimitsOpen = false;
  isSaving = false;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private nfsService: NfsService,
    private notificationService: NotificationService,
    private nfsClusterResourceStateService: NfsClusterResourceStateService,
    private dimlessBinaryPerSecond: DimlessBinaryPerSecondPipe,
    private formatterService: FormatterService
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

    this.nfsClusterResourceStateService.state$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((state: NfsClusterState) => {
        this.qos = state.qos;
        this.applyCluster(state.cluster);
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
    this.overviewFields = cluster ? this.buildOverviewFields(cluster) : [];
  }

  openPerformanceLimits(): void {
    this.isPerformanceLimitsOpen = true;
  }

  closePerformanceLimits(): void {
    this.isPerformanceLimitsOpen = false;
  }

  savePerformanceLimits(): void {
    if (!this.selection || !this.nfsRateLimitComponent) {
      return;
    }

    if (!this.nfsRateLimitComponent.showValidationErrors()) {
      return;
    }

    this.isSaving = true;

    const clusterFormObj = { cluster_id: this.selection.name };
    let ratelimitBw: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitFormValue();
    let ratelimitOpsValue: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitOpsFormValue();

    ratelimitBw = {
      ...ratelimitBw,
      ...clusterFormObj,
      disable_qos: !ratelimitBw?.enable_qos
    };
    delete ratelimitBw.enable_qos;

    ratelimitOpsValue = {
      ...ratelimitOpsValue,
      ...clusterFormObj,
      disable_ops: !ratelimitOpsValue?.enable_ops
    };
    delete ratelimitOpsValue.enable_ops;

    forkJoin([
      this.nfsService.enableQosBandwidthForCLuster(ratelimitBw),
      this.nfsService.enableQosOpsForCLuster(ratelimitOpsValue)
    ]).subscribe({
      next: () => {
        const notificationTitle = $localize`Cluster QoS Type Configured for '${this.selection.name}'`;
        this.notificationService.show(NotificationType.success, notificationTitle);
        this.nfsClusterResourceStateService.load(this.clusterId);
        this.isSaving = false;
        this.closePerformanceLimits();
      },
      error: () => {
        this.isSaving = false;
      }
    });
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

    const iopsOn = this.qos?.enable_iops_control ?? false;
    const bwOn = this.qos?.enable_bw_control ?? false;

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
      { label: $localize`Placement`, value: this.formatPlacement(cluster.placement) },
      {
        label: $localize`IOPS Limit`,
        value: iopsOn ? $localize`On` : $localize`Off`,
        tooltip: this.buildIopsTooltip()
      },
      {
        label: $localize`Bandwidth limit`,
        value: bwOn ? $localize`On` : $localize`Off`,
        tooltip: this.buildBandwidthTooltip()
      }
    ];
  }

  private buildIopsTooltip(): string | undefined {
    if (!this.qos?.enable_iops_control) {
      return undefined;
    }
    const lines: string[] = [];
    const qosType = this.qos.qos_type || QOSType.PerShare;

    if (qosType === QOSType.PerShare || qosType === QOSType.PerSharePerClient) {
      if (this.qos.max_export_iops != null) {
        lines.push($localize`Shares max. IOPs: ${this.qos.max_export_iops}`);
      }
    }
    if (qosType === QOSType.PerClient || qosType === QOSType.PerSharePerClient) {
      if (this.qos.max_client_iops != null) {
        lines.push($localize`Clients max. IOPs: ${this.qos.max_client_iops}`);
      }
    }

    return lines.length > 0 ? lines.join('\n') : undefined;
  }

  private buildBandwidthTooltip(): string | undefined {
    if (!this.qos?.enable_bw_control) {
      return undefined;
    }
    const lines: string[] = [];
    const qosType = this.qos.qos_type || QOSType.PerShare;

    if (qosType === QOSType.PerShare || qosType === QOSType.PerSharePerClient) {
      lines.push($localize`Shares max. bandwidth:`);
      lines.push($localize`Read: ${this.formatBandwidth(this.qos.max_export_read_bw)}`);
      lines.push($localize`Write: ${this.formatBandwidth(this.qos.max_export_write_bw)}`);
    }
    if (qosType === QOSType.PerClient || qosType === QOSType.PerSharePerClient) {
      lines.push($localize`Clients max. bandwidth:`);
      lines.push($localize`Read: ${this.formatBandwidth(this.qos.max_client_read_bw)}`);
      lines.push($localize`Write: ${this.formatBandwidth(this.qos.max_client_write_bw)}`);
    }

    return lines.length > 0 ? lines.join('\n') : undefined;
  }

  private formatBandwidth(value: any): string {
    if (value == null || value === '') {
      return '-';
    }
    const bytes = typeof value === 'number' ? value : this.formatterService.toBytes(String(value));
    if (bytes == null || isNaN(bytes)) {
      return String(value);
    }
    return this.dimlessBinaryPerSecond.transform(bytes);
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
