import { HttpParams } from '@angular/common/http';
import { Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription, forkJoin } from 'rxjs';

import { OsdService } from '~/app/shared/api/osd.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import {
  Osd,
  OsdCapacityOverviewModel,
  OsdDetails,
  OsdHistoryRatePoint,
  OsdIoOverviewModel
} from '~/app/shared/models/osd.model';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { FormatterService } from '~/app/shared/services/formatter.service';

@Component({
  selector: 'cd-osd-details',
  templateUrl: './osd-resource-page.component.html',
  styleUrls: ['./osd-resource-page.component.scss'],
  standalone: false
})
export class OsdResourcePageComponent implements OnInit, OnChanges, OnDestroy {
  @Input()
  selection: Osd | null;

  private sub = new Subscription();
  private readonly disabledFlags: string[] = [
    'sortbitwise',
    'purged_snapdirs',
    'recovery_deletes',
    'pglog_hardlimit'
  ];
  private readonly indivFlagNames: string[] = ['noup', 'nodown', 'noin', 'noout'];

  osdId: number | null = null;
  section = '';
  osdOverviewFields: OverviewField[] = [];
  osdMap: Record<string, unknown> = {};
  osdMetadata: Record<string, unknown> = {};
  osd: Osd | null;
  capacityOverviewModel: OsdCapacityOverviewModel = {
    name: '',
    usageTotal: 0,
    usageUsed: null,
    usagePercent: '',
    usedCapacity: '',
    availableCapacity: '',
    totalCapacity: ''
  };
  ioOverviewModel: OsdIoOverviewModel = {
    readBytes: '-',
    writeBytes: '-',
    readOps: '-',
    writeOps: '-',
    readBytesChartData: [],
    writeBytesChartData: []
  };
  grafanaPermission: Permission;

  constructor(
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private route: ActivatedRoute,
    private formatter: FormatterService
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';

    const paramMap$ = this.route.parent?.paramMap ?? this.route.paramMap;
    this.sub.add(
      paramMap$.subscribe((pm: ParamMap) => {
        const idFromRoute = Number(pm.get('id'));
        if (Number.isFinite(idFromRoute)) {
          this.loadFromRoute(idFromRoute);
          return;
        }

        if (Number.isFinite(this.selection?.id)) {
          this.loadFromSelection(this.selection);
          return;
        }

        this.resetViewModel();
      })
    );
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!changes['selection']) {
      return;
    }

    const idFromRoute = Number(this.route.parent?.snapshot?.paramMap?.get('id'));
    if (Number.isFinite(idFromRoute)) {
      return;
    }

    if (Number.isFinite(this.selection?.id)) {
      this.loadFromSelection(this.selection);
      return;
    }

    this.resetViewModel();
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  refresh() {
    if (!Number.isFinite(this.osdId)) {
      return;
    }

    this.sub.add(
      this.osdService.getDetails(this.osdId as number).subscribe((data) => {
        this.osdMap = data?.osd_map || {};
        this.osdMetadata = data?.osd_metadata || {};
      })
    );
  }

  private loadFromRoute(id: number): void {
    this.osdId = id;

    const params = new HttpParams().set('offset', '0').set('limit', '10000').set('sort', '+id');
    this.sub.add(
      forkJoin([
        this.osdService.getList(params).observable,
        this.osdService.getFlags(),
        this.osdService.getDetails(id)
      ]).subscribe(([osds, clusterFlags, details]) => {
        const clusterFlagList = Array.isArray(clusterFlags) ? clusterFlags : [];
        const row = (osds || []).find((item: Osd) => item.id === id);
        if (!row) {
          this.resetViewModel();
          return;
        }

        const normalizedOsd = this.normalizeOsd(row, clusterFlagList);
        this.applyViewModel(normalizedOsd, details);
      })
    );
  }

  private loadFromSelection(selection: Osd): void {
    const selectedId = Number(selection?.id);
    if (!Number.isFinite(selectedId)) {
      this.resetViewModel();
      return;
    }

    this.osdId = selectedId;
    this.sub.add(
      forkJoin([this.osdService.getFlags(), this.osdService.getDetails(selectedId)]).subscribe(
        ([clusterFlags, details]) => {
          const clusterFlagList = Array.isArray(clusterFlags) ? clusterFlags : [];
          const normalizedOsd = this.normalizeOsd(selection, clusterFlagList);
          this.applyViewModel(normalizedOsd, details);
        }
      )
    );
  }

  private applyViewModel(normalizedOsd: Osd, details: OsdDetails): void {
    this.osd = normalizedOsd;
    this.osdMap = details?.osd_map || {};
    this.osdMetadata = details?.osd_metadata || {};
    this.osdOverviewFields = this.buildOverviewFields(normalizedOsd);
    this.capacityOverviewModel = this.buildCapacityOverview(normalizedOsd);
    this.ioOverviewModel = this.buildIoOverview(normalizedOsd);
  }

  private buildCapacityOverview(osd: Osd): OsdCapacityOverviewModel {
    const total = Number.isFinite(osd?.stats?.stat_bytes) ? Number(osd.stats.stat_bytes) : 0;
    const used = Number.isFinite(osd?.stats?.stat_bytes_used)
      ? Number(osd.stats.stat_bytes_used)
      : null;
    const available = used !== null && total > 0 ? Math.max(total - used, 0) : null;
    const usagePercent =
      total > 0 && used !== null ? `${Math.round((used / total) * 1000) / 10}%` : '';

    return {
      name: `osd ${osd?.id ?? ''}`,
      usageTotal: total,
      usageUsed: used,
      usagePercent,
      usedCapacity: this.formatter.formatToBinary(used, false, 1),
      availableCapacity: this.formatter.formatToBinary(available, false, 1),
      totalCapacity: this.formatter.formatToBinary(total, false, 1)
    };
  }

  private normalizeOsd(osd: Osd, clusterFlags: string[]): Osd {
    const normalized = { ...osd };
    normalized.collectedStates = this.collectStates(normalized);
    normalized.stats.usage =
      Number(normalized.stats.stat_bytes) > 0
        ? Number(normalized.stats.stat_bytes_used) / Number(normalized.stats.stat_bytes)
        : 0;
    normalized.cdIsBinary = true;
    normalized.cdIndivFlags = (normalized.state || []).filter((flag: string) =>
      this.indivFlagNames.includes(flag)
    );
    normalized.cdClusterFlags = clusterFlags.filter(
      (flag: string) => !this.disabledFlags.includes(flag)
    );

    return normalized;
  }

  private buildOverviewFields(osd: Osd): OverviewField[] {
    const flags = [...(osd?.cdClusterFlags || []), ...(osd?.cdIndivFlags || [])];

    return [
      { label: $localize`ID`, value: osd?.id },
      { label: $localize`Host`, value: osd?.host?.name },
      { label: $localize`Status`, values: osd?.collectedStates || [], type: 'tags' },
      { label: $localize`Device Class`, value: osd?.tree?.device_class },
      { label: $localize`PGs`, value: osd?.stats?.numpg },
      { label: $localize`Flags`, values: flags, type: 'tags' }
    ];
  }

  private buildIoOverview(osd: Osd): OsdIoOverviewModel {
    return {
      readBytes: this.formatLatestHistoryValue(osd?.stats_history?.op_out_bytes),
      writeBytes: this.formatLatestHistoryValue(osd?.stats_history?.op_in_bytes),
      readOps: this.formatOpsValue(osd?.stats?.op_r),
      writeOps: this.formatOpsValue(osd?.stats?.op_w),
      readBytesChartData: this.getHistoryChartData(osd?.stats_history?.op_out_bytes, 'Read Bytes'),
      writeBytesChartData: this.getHistoryChartData(osd?.stats_history?.op_in_bytes, 'Write Bytes')
    };
  }

  private getHistoryChartData(
    history: OsdHistoryRatePoint[] | undefined,
    groupLabel: string
  ): ChartPoint[] {
    if (!Array.isArray(history) || history.length === 0) {
      return [];
    }

    const fallbackStart = Date.now() - Math.max(history.length - 1, 0) * 60000;

    return history.reduce((points: ChartPoint[], point: OsdHistoryRatePoint, index: number) => {
      const fallbackTimestamp = new Date(fallbackStart + index * 60000);

      if (Array.isArray(point)) {
        const rawTimestamp = Number(point[0]);
        const rawValue = Number(point[1]);
        if (Number.isFinite(rawValue)) {
          points.push({
            timestamp: Number.isFinite(rawTimestamp)
              ? new Date(rawTimestamp * 1000)
              : fallbackTimestamp,
            values: { [groupLabel]: rawValue }
          });
        }
        return points;
      }

      const rawValue = Number(point);
      if (Number.isFinite(rawValue)) {
        points.push({
          timestamp: fallbackTimestamp,
          values: { [groupLabel]: rawValue }
        });
      }

      return points;
    }, []);
  }

  private collectStates(osd: Osd): string[] {
    const states = [osd?.['in'] ? 'in' : 'out'];
    if (osd?.['up']) {
      states.push('up');
    } else if ((osd?.state || []).includes('destroyed')) {
      states.push('destroyed');
    } else {
      states.push('down');
    }
    return states;
  }

  private formatLatestHistoryValue(history: OsdHistoryRatePoint[] | undefined): string {
    if (!Array.isArray(history) || history.length === 0) {
      return '-';
    }

    const latestEntry = history[history.length - 1];
    const latestValue = Array.isArray(latestEntry) ? Number(latestEntry[1]) : Number(latestEntry);
    if (!Number.isFinite(latestValue)) {
      return '-';
    }

    return this.formatter.formatToBinary(latestValue, false, 1);
  }

  private formatOpsValue(value: number): string {
    if (!Number.isFinite(value)) {
      return '-';
    }

    return `${Math.round(value * 10) / 10}/s`;
  }

  private resetViewModel(): void {
    this.osdId = null;
    this.osd = null;
    this.osdOverviewFields = [];
    this.osdMap = {};
    this.osdMetadata = {};
    this.capacityOverviewModel = {
      name: '',
      usageTotal: 0,
      usageUsed: null,
      usagePercent: '',
      usedCapacity: '',
      availableCapacity: '',
      totalCapacity: ''
    };
    this.ioOverviewModel = {
      readBytes: '-',
      writeBytes: '-',
      readOps: '-',
      writeOps: '-',
      readBytesChartData: [],
      writeBytesChartData: []
    };
  }
}
