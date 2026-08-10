import { Component, Input, OnChanges, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Subscription } from 'rxjs';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { ChartPoint } from '~/app/shared/models/area-chart-point';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss'],
  standalone: false
})
export class RgwDaemonDetailsComponent implements OnChanges, OnInit, OnDestroy {
  metadata: any;
  serviceId = '';
  serviceMapId = '';
  grafanaPermission: Permission;

  requestsChartData: ChartPoint[] = [];
  latencyChartData: ChartPoint[] = [];
  bandwidthChartData: ChartPoint[] = [];
  
  private interval = new Subscription();
  private timerGetPrometheusDataSub: Subscription;
  private lastTimeSpan: { start: number; end: number; step: number };

  @Input()
  selection: RgwDaemon;

  constructor(
    private rgwDaemonService: RgwDaemonService,
    private authStorageService: AuthStorageService,
    private prometheusService: PrometheusService,
    private refreshIntervalService: RefreshIntervalService
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnInit() {
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.getMetaData();
      if (this.lastTimeSpan) {
        this.getPrometheusData(this.lastTimeSpan);
      }
    });
  }

  ngOnChanges() {
    if (this.selection) {
      this.serviceId = this.selection.id;
      this.serviceMapId = this.selection.service_map_id;
      this.getMetaData();
      if (this.lastTimeSpan) {
        this.getPrometheusData(this.lastTimeSpan);
      }
    }
  }

  ngOnDestroy() {
    if (this.interval) {
      this.interval.unsubscribe();
    }
    if (this.timerGetPrometheusDataSub) {
      this.timerGetPrometheusDataSub.unsubscribe();
    }
  }

  getMetaData() {
    if (_.isEmpty(this.serviceId)) {
      return;
    }
    this.rgwDaemonService.get(this.serviceId).subscribe((resp: any) => {
      this.metadata = resp['rgw_metadata'];
    });
  }

  getPrometheusData(time: { start: number; end: number; step: number }) {
    if (_.isEmpty(this.serviceMapId)) return;
    this.lastTimeSpan = time;
    
    // Extract instance_id from serviceId (e.g., 'data123.ceph-node-00.dpqvin' -> 'dpqvin')
    const instanceId = this.serviceId.split('.').pop();
    const customQueries = {
      RGW_REQUEST_PER_SECOND: `sum(rate(ceph_rgw_req{instance_id="${instanceId}"}[1m]))`,
      AVG_GET_LATENCY: `(sum(rate(ceph_rgw_op_get_obj_lat_sum{instance_id="${instanceId}"}[1m])) / sum(rate(ceph_rgw_op_get_obj_lat_count{instance_id="${instanceId}"}[1m]))) * 1000`,
      AVG_PUT_LATENCY: `(sum(rate(ceph_rgw_op_put_obj_lat_sum{instance_id="${instanceId}"}[1m])) / sum(rate(ceph_rgw_op_put_obj_lat_count{instance_id="${instanceId}"}[1m]))) * 1000`,
      GET_BANDWIDTH: `sum(rate(ceph_rgw_op_get_obj_bytes{instance_id="${instanceId}"}[1m]))`,
      PUT_BANDWIDTH: `sum(rate(ceph_rgw_op_put_obj_bytes{instance_id="${instanceId}"}[1m]))`
    };

    if (this.timerGetPrometheusDataSub) {
      this.timerGetPrometheusDataSub.unsubscribe();
    }
    
    this.timerGetPrometheusDataSub = this.prometheusService
      .getRangeQueriesData(time, customQueries, true)
      .subscribe((data) => {
        this.requestsChartData = [
          ...this.toSeries(data['RGW_REQUEST_PER_SECOND'] || [], 'Total Requests')
        ];
        this.latencyChartData = [
          ...this.toSeries(data['AVG_GET_LATENCY'] || [], 'GET Latency'),
          ...this.toSeries(data['AVG_PUT_LATENCY'] || [], 'PUT Latency')
        ];
        this.bandwidthChartData = [
          ...this.toSeries(data['GET_BANDWIDTH'] || [], 'GET Bandwidth'),
          ...this.toSeries(data['PUT_BANDWIDTH'] || [], 'PUT Bandwidth')
        ];
      });
  }

  private toSeries(metric: [number, string][], label: string): ChartPoint[] {
    return metric.map(([ts, val]) => ({
      timestamp: new Date(ts * 1000),
      values: { [label]: Number(val) }
    }));
  }
}
