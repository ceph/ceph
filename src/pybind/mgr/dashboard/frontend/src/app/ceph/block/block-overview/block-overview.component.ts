import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { Subscription } from 'rxjs';

import { RbdService } from '~/app/shared/api/rbd.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { SummaryService } from '~/app/shared/services/summary.service';

export interface BlockOverviewModel {
  totalPools: number;
  totalImages: number;
  totalSnapshots: number;
  totalProvisionedBytes: number;
  totalUsedBytes: number;
  usagePercent: string;
  usedCapacity: string;
  provisionedCapacity: string;
  mirroringWarnings: number;
  mirroringErrors: number;
  iopsChartData: ChartPoint[];
  throughputChartData: ChartPoint[];
  latencyChartData: ChartPoint[];
}

@Component({
  selector: 'cd-block-overview',
  templateUrl: './block-overview.component.html',
  styleUrls: ['./block-overview.component.scss'],
  standalone: false
})
export class BlockOverviewComponent implements OnInit, OnDestroy {
  private rbdService = inject(RbdService);
  private prometheusService = inject(PrometheusService);
  private performanceCardService = inject(PerformanceCardService);
  private formatter = inject(FormatterService);
  private summaryService = inject(SummaryService);

  private sub = new Subscription();
  private readonly binaryUnits = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

  overviewFields: OverviewField[] = [];
  model: BlockOverviewModel = {
    totalPools: 0,
    totalImages: 0,
    totalSnapshots: 0,
    totalProvisionedBytes: 0,
    totalUsedBytes: 0,
    usagePercent: '',
    usedCapacity: '',
    provisionedCapacity: '',
    mirroringWarnings: 0,
    mirroringErrors: 0,
    iopsChartData: [],
    throughputChartData: [],
    latencyChartData: []
  };

  ngOnInit() {
    this.loadSummaryData();
    this.loadBlockData();
    this.loadPerformanceData();
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadSummaryData() {
    this.sub.add(
      this.summaryService.subscribe((summary: any) => {
        const mirroringData = summary?.rbd_mirroring || {};
        this.model = {
          ...this.model,
          mirroringWarnings: mirroringData?.warnings || 0,
          mirroringErrors: mirroringData?.errors || 0
        };
        this.overviewFields = this.buildOverviewFields();
      })
    );
  }

  private loadBlockData() {
    this.sub.add(
      this.rbdService.list({}).subscribe((pools: any[]) => {
        const poolList: any[] = pools || [];
        let totalImages = 0;
        let totalSnapshots = 0;
        let totalProvisioned = 0;
        let totalUsed = 0;

        poolList.forEach((pool: any) => {
          const images = pool.value || [];
          totalImages += images.length;
          images.forEach((img: any) => {
            totalProvisioned += img.size || 0;
            totalUsed += img.disk_usage || 0;
            totalSnapshots += img.snapshots?.length || 0;
          });
        });

        this.model = {
          ...this.model,
          totalPools: poolList.length,
          totalImages,
          totalSnapshots,
          totalProvisionedBytes: totalProvisioned,
          totalUsedBytes: totalUsed,
          usagePercent:
            totalProvisioned > 0
              ? `${Math.round((totalUsed / totalProvisioned) * 1000) / 10}%`
              : '0%',
          usedCapacity: this.formatBytes(totalUsed),
          provisionedCapacity: this.formatBytes(totalProvisioned)
        };
        this.overviewFields = this.buildOverviewFields();
      })
    );
  }

  public loadPerformanceData(time: any = this.prometheusService.lastHourDateObject) {
    const queries = {
      READ_OPS: 'sum(rate(ceph_pool_rd{application="rbd"}[1m]))',
      WRITE_OPS: 'sum(rate(ceph_pool_wr{application="rbd"}[1m]))',
      READ_BYTES: 'sum(rate(ceph_pool_rd_bytes{application="rbd"}[1m]))',
      WRITE_BYTES: 'sum(rate(ceph_pool_wr_bytes{application="rbd"}[1m]))',
      READ_LATENCY: 'avg_over_time(ceph_osd_apply_latency_ms[1m])',
      WRITE_LATENCY: 'avg_over_time(ceph_osd_commit_latency_ms[1m])'
    };

    this.sub.add(
      this.prometheusService.getRangeQueriesData(time, queries, true).subscribe((raw: any) => {
        const iopsChartData = this.performanceCardService.mergeSeries(
          this.performanceCardService.toSeries(raw?.READ_OPS || [], 'Read'),
          this.performanceCardService.toSeries(raw?.WRITE_OPS || [], 'Write')
        );
        const finalIopsData = iopsChartData.length
          ? iopsChartData
          : [{ timestamp: new Date(), values: { Read: 0, Write: 0 } }];

        const throughputChartData = this.performanceCardService.mergeSeries(
          this.performanceCardService.toSeries(raw?.READ_BYTES || [], 'Read'),
          this.performanceCardService.toSeries(raw?.WRITE_BYTES || [], 'Write')
        );
        const finalThroughputData = throughputChartData.length
          ? throughputChartData
          : [{ timestamp: new Date(), values: { Read: 0, Write: 0 } }];

        const latencyChartData = this.performanceCardService.mergeSeries(
          this.performanceCardService.toSeries(raw?.READ_LATENCY || [], 'Read (Avg)'),
          this.performanceCardService.toSeries(raw?.WRITE_LATENCY || [], 'Write (Avg)')
        );
        const finalLatencyData = latencyChartData.length
          ? latencyChartData
          : [{ timestamp: new Date(), values: { 'Read (Avg)': 0, 'Write (Avg)': 0 } }];

        this.model = {
          ...this.model,
          iopsChartData: finalIopsData,
          throughputChartData: finalThroughputData,
          latencyChartData: finalLatencyData
        };
      })
    );
  }

  private buildOverviewFields(): OverviewField[] {
    return [
      {
        label: $localize`Total Images`,
        value: `${this.model.totalImages}`,
        path: '/block/rbd'
      },
      {
        label: $localize`Total Pools`,
        value: `${this.model.totalPools}`,
        path: '/pool'
      },
      {
        label: $localize`Total Snapshots`,
        value: `${this.model.totalSnapshots}`
      },
      {
        label: $localize`Mirroring`,
        value:
          this.model.mirroringErrors > 0
            ? $localize`Error`
            : this.model.mirroringWarnings > 0
              ? $localize`Warning`
              : $localize`Healthy`,
        type: 'status',
        status:
          this.model.mirroringErrors > 0
            ? 'danger'
            : this.model.mirroringWarnings > 0
              ? 'warning'
              : 'success'
      }
    ];
  }

  private formatBytes(value?: number): string {
    return this.formatter.format_number(value, 1024, this.binaryUnits, 1);
  }
}
