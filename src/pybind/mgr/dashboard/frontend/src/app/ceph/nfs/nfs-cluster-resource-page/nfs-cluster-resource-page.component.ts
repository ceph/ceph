import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { concat as observableConcat, Subscription } from 'rxjs';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NFSBwIopConfig, NFSCluster } from '../models/nfs-cluster-config';
import { NfsRateLimitComponent } from '../nfs-rate-limit/nfs-rate-limit.component';

@Component({
  selector: 'cd-nfs-cluster-resource-page',
  templateUrl: './nfs-cluster-resource-page.component.html',
  styleUrls: ['./nfs-cluster-resource-page.component.scss'],
  standalone: false
})
export class NfsClusterResourcePageComponent implements OnInit, OnDestroy {
  @ViewChild(NfsRateLimitComponent, { static: false })
  nfsRateLimitComponent: NfsRateLimitComponent;

  private sub = new Subscription();

  section = '';
  clusterId = '';
  selection: NFSCluster | undefined;
  loadError = false;
  isOverviewLoading = false;
  overviewFields: OverviewField[] = [];
  sidePanelOpen = false;
  qosConfig: NFSBwIopConfig = {};

  private bwUnits = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s'];

  constructor(
    private route: ActivatedRoute,
    private nfsService: NfsService,
    private notificationService: NotificationService,
    private nfsClusterResourceStateService: NfsClusterResourceStateService,
    private formatter: FormatterService
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

  openPerformanceLimits(): void {
    if (this.selection) {
      this.nfsService.getClusterBandwidthOpsConfig(this.selection.name).subscribe({
        next: (qos: NFSBwIopConfig) => {
          this.qosConfig = qos;
          this.sidePanelOpen = true;
        },
        error: () => {
          this.qosConfig = {};
          this.sidePanelOpen = true;
        }
      });
    }
  }

  closeSidePanel(): void {
    this.sidePanelOpen = false;
  }

  savePerformanceLimits(): void {
    if (!this.nfsRateLimitComponent || !this.selection) {
      return;
    }
    if (!this.nfsRateLimitComponent.showValidationErrors()) {
      return;
    }
    const clusterName = this.selection.name;
    let ratelimitBw: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitFormValue();
    let ratelimitOps: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitOpsFormValue();

    ratelimitBw = {
      ...ratelimitBw,
      cluster_id: clusterName,
      disable_qos: !ratelimitBw?.enable_qos
    };
    delete ratelimitBw.enable_qos;

    ratelimitOps = {
      ...ratelimitOps,
      cluster_id: clusterName,
      disable_ops: !ratelimitOps?.enable_ops
    };
    delete ratelimitOps.enable_ops;

    observableConcat(
      this.nfsService.enableQosBandwidthForCLuster(ratelimitBw),
      this.nfsService.enableQosOpsForCLuster(ratelimitOps)
    ).subscribe({
      next: () => {},
      error: (err) => {
        const detail = err?.error?.detail;
        this.notificationService.show(
          NotificationType.error,
          detail || $localize`Failed to update performance limits for '${clusterName}'.`
        );
      },
      complete: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Performance limits configured for '${clusterName}'.`
        );
        this.closeSidePanel();
        this.nfsClusterResourceStateService.load(this.clusterId);
      }
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

    if (cluster) {
      this.overviewFields = this.buildOverviewFields(cluster);
      this.nfsService.getClusterBandwidthOpsConfig(cluster.name).subscribe({
        next: (qos: NFSBwIopConfig) => {
          this.overviewFields = this.buildOverviewFields(cluster, qos);
        }
      });
    } else {
      this.overviewFields = [];
    }
  }

  private buildOverviewFields(cluster: NFSCluster, qos?: NFSBwIopConfig): OverviewField[] {
    const isActive = (cluster.backend || []).some((b) => !b.status || b.status === 'running');
    const haEnabled =
      !!cluster.ingress_mode ||
      ['active-active', 'active-passive'].includes(cluster.deployment_type || '');

    const fields: OverviewField[] = [
      { label: $localize`Name`, value: cluster.name },
      {
        label: $localize`Status`,
        value: isActive ? $localize`Active` : $localize`Inactive`,
        type: 'status',
        status: isActive ? 'success' : 'warning'
      },
      {
        label: $localize`High availability`,
        value: haEnabled ? $localize`Enabled` : $localize`Disabled`
      },
      { label: $localize`Monitoring port`, value: cluster.monitor_port ?? '-' },
      {
        label: $localize`IOPS limit`,
        value: qos?.enable_iops_control ? $localize`On` : $localize`Off`,
        helperText: this.formatIopsHelper(qos)
      },
      {
        label: $localize`Bandwidth limit`,
        value: qos?.enable_bw_control ? $localize`On` : $localize`Off`,
        helperText: this.formatBwHelper(qos)
      }
    ];
    return fields;
  }

  private formatBw(bytes?: number): string {
    if (!bytes) {
      return '-';
    }
    return this.formatter.format_number(bytes, 1024, this.bwUnits, 0);
  }

  private formatBwHelper(qos?: NFSBwIopConfig): string {
    if (!qos?.enable_bw_control) {
      return '';
    }
    const lines: string[] = [];
    if (qos.max_export_read_bw || qos.max_export_write_bw) {
      lines.push($localize`Shares max. bandwidth:`);
      lines.push($localize`Read: ${this.formatBw(qos.max_export_read_bw)}`);
      lines.push($localize`Write: ${this.formatBw(qos.max_export_write_bw)}`);
    }
    if (qos.max_client_read_bw || qos.max_client_write_bw) {
      lines.push($localize`Clients max. bandwidth:`);
      lines.push($localize`Read: ${this.formatBw(qos.max_client_read_bw)}`);
      lines.push($localize`Write: ${this.formatBw(qos.max_client_write_bw)}`);
    }
    return lines.join('\n');
  }

  private formatIopsHelper(qos?: NFSBwIopConfig): string {
    if (!qos?.enable_iops_control) {
      return '';
    }
    const lines: string[] = [];
    if (qos.max_export_iops) {
      lines.push($localize`Shares max. IOPs: ${qos.max_export_iops}`);
    }
    if (qos.max_client_iops) {
      lines.push($localize`Clients max. IOPs: ${qos.max_client_iops}`);
    }
    return lines.join('\n');
  }

}
