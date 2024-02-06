import { Component, OnInit, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalService } from '~/app/shared/services/modal.service';
import { MultiClusterFormComponent } from './multi-cluster-form/multi-cluster-form.component';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { MultiClusterPromqls as queries } from '~/app/shared/enum/dashboard-promqls.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-multi-cluster',
  templateUrl: './multi-cluster.component.html',
  styleUrls: ['./multi-cluster.component.scss']
})
export class MultiClusterComponent implements OnInit {
  COUNT_OF_UTILIZATION_CHARTS = 5;

  @ViewChild('nameTpl', { static: true })
  nameTpl: any;

  columns: Array<CdTableColumn> = [];

  queriesResults: any = {
    ALERTS_COUNT: 0,
    CLUSTER_COUNT: 0,
    HEALTH_OK_COUNT: 0,
    HEALTH_WARNING_COUNT: 0,
    HEALTH_ERROR_COUNT: 0,
    TOTAL_CLUSTERS_CAPACITY: 0,
    TOTAL_USED_CAPACITY: 0,
    CLUSTER_CAPACITY_UTILIZATION: 0,
    CLUSTER_IOPS_UTILIZATION: 0,
    CLUSTER_THROUGHPUT_UTILIZATION: 0,
    POOL_CAPACITY_UTILIZATION: 0,
    POOL_IOPS_UTILIZATION: 0,
    POOL_THROUGHPUT_UTILIZATION: 0,
    TOTAL_CAPACITY: 0,
    USED_CAPACITY: 0,
    HOSTS: 0,
    POOLS: 0,
    OSDS: 0,
    CLUSTER_ALERTS: 0,
    version: ''
  };
  alerts: any;

  private subs = new Subscription();
  dashboardClustersMap: Map<string, string> = new Map<string, string>();
  icons = Icons;
  loading = true;
  bsModalRef: NgbModalRef;
  isMultiCluster = true;
  clusterTokenStatus: object = {};
  localClusterName: string;
  clusters: any;
  connectionErrorsCount = 0;

  capacityLabels: string[] = [];
  iopsLabels: string[] = [];
  throughputLabels: string[] = [];
  poolIOPSLabels: string[] = [];
  poolCapacityLabels: string[] = [];
  poolThroughputLabels: string[] = [];

  capacityValues: string[] = [];
  iopsValues: string[] = [];
  throughputValues: string[] = [];
  poolIOPSValues: string[] = [];
  poolCapacityValues: string[] = [];
  poolThroughputValues: string[] = [];

  constructor(
    private multiClusterService: MultiClusterService,
    private modalService: ModalService,
    private prometheusService: PrometheusService,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  ngOnInit(): void {
    this.columns = [
      {
        prop: 'cluster',
        name: $localize`Cluster Name`,
        flexGrow: 2,
        cellTemplate: this.nameTpl
      },
      {
        prop: 'cluster_connection_status',
        name: $localize`Connection`,
        flexGrow: 2,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            1: { value: 'DISCONNECTED', class: 'badge-danger' },
            0: { value: 'CONNECTED', class: 'badge-success' },
            2: { value: 'CHECKING..', class: 'badge-info' }
          }
        }
      },
      {
        prop: 'status',
        name: $localize`Status`,
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            1: { value: 'WARN', class: 'badge-warning' },
            0: { value: 'OK', class: 'badge-success' },
            2: { value: 'ERROR', class: 'badge-danger' }
          }
        }
      },
      { prop: 'alert', name: $localize`Alerts`, flexGrow: 1 },
      { prop: 'version', name: $localize`Version`, flexGrow: 2 },
      {
        prop: 'total_capacity',
        name: $localize`Total Capacity`,
        pipe: this.dimlessBinaryPipe,
        flexGrow: 1
      },
      {
        prop: 'used_capacity',
        name: $localize`Used Capacity`,
        pipe: this.dimlessBinaryPipe,
        flexGrow: 1
      },
      {
        prop: 'available_capacity',
        name: $localize`Available Capacity`,
        pipe: this.dimlessBinaryPipe,
        flexGrow: 1
      },
      { prop: 'pools', name: $localize`Pools`, flexGrow: 1 },
      { prop: 'hosts', name: $localize`Hosts`, flexGrow: 1 },
      { prop: 'osds', name: $localize`OSDs`, flexGrow: 1 }
    ];

    this.subs.add(
      this.multiClusterService.subscribe((resp: any) => {
        this.isMultiCluster = Object.keys(resp['config']).length > 1;
        const hubUrl = resp['hub_url'];
        for (const key in resp['config']) {
          if (resp['config'].hasOwnProperty(key)) {
            const cluster = resp['config'][key][0];
            if (hubUrl === cluster.url) {
              this.localClusterName = cluster.name;
              break;
            }
          }
        }
      })
    );

    this.subs.add(
      this.multiClusterService.subscribeClusterTokenStatus((resp: object) => {
        this.clusterTokenStatus = resp;
      })
    );
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
  }

  openRemoteClusterInfoModal() {
    const initialState = {
      action: 'connect'
    };
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, initialState, {
      size: 'lg'
    });
  }

  getPrometheusData(selectedTime: any) {
    this.prometheusService
      .getMultiClusterQueriesData(selectedTime, queries, this.queriesResults)
      .subscribe((data: any) => {
        this.queriesResults = data;
        this.loading = false;
        this.alerts = this.queriesResults.ALERTS;
        this.getAlertsInfo();
        this.getClustersInfo();
      });
  }

  getAlertsInfo() {
    interface Alert {
      alertName: string;
      alertState: string;
      severity: string;
      cluster: string;
    }

    const alerts: Alert[] = [];

    this.alerts?.forEach((item: any) => {
      const metric = item.metric;
      const alert: Alert = {
        alertName: metric.alertname,
        cluster: metric.cluster,
        alertState: metric.alertstate,
        severity: metric.severity
      };
      alerts.push(alert);
    });

    this.alerts = alerts;
  }

  getClustersInfo() {
    interface ClusterInfo {
      cluster: string;
      status: number;
      alert: number;
      total_capacity: number;
      used_capacity: number;
      available_capacity: number;
      pools: number;
      osds: number;
      hosts: number;
      version: string;
      cluster_connection_status: number;
    }

    const clusters: ClusterInfo[] = [];

    this.queriesResults.TOTAL_CAPACITY?.forEach((totalCapacityMetric: any) => {
      const clusterName = totalCapacityMetric.metric.cluster;
      const totalCapacity = parseInt(totalCapacityMetric.value[1]);
      const getMgrMetadata = this.findCluster(this.queriesResults?.MGR_METADATA, clusterName);
      const version = this.getVersion(getMgrMetadata.metric.ceph_version);

      const usedCapacity = this.findClusterData(this.queriesResults?.USED_CAPACITY, clusterName);
      const pools = this.findClusterData(this.queriesResults?.POOLS, clusterName);
      const hosts = this.findClusterData(this.queriesResults?.HOSTS, clusterName);
      const alert = this.findClusterData(this.queriesResults?.CLUSTER_ALERTS, clusterName);
      const osds = this.findClusterData(this.queriesResults?.OSDS, clusterName);
      const status = this.findClusterData(this.queriesResults?.HEALTH_STATUS, clusterName);
      const available_capacity = totalCapacity - usedCapacity;

      clusters.push({
        cluster: clusterName,
        status,
        alert,
        total_capacity: totalCapacity,
        used_capacity: usedCapacity,
        available_capacity: available_capacity,
        pools,
        osds,
        hosts,
        version,
        cluster_connection_status: 2
      });
    });

    if (this.clusterTokenStatus) {
      clusters.forEach((cluster: any) => {
        cluster.cluster_connection_status = this.clusterTokenStatus[cluster.cluster]?.status;
        if (cluster.cluster === this.localClusterName) {
          cluster.cluster_connection_status = 0;
        }
      });
      this.connectionErrorsCount = clusters.filter(
        (cluster) => cluster.cluster_connection_status === 1
      ).length;
    }

    this.clusters = clusters;

    // Generate labels and metrics for utilization charts
    this.capacityLabels = this.generateQueryLabel(this.queriesResults.CLUSTER_CAPACITY_UTILIZATION);
    this.iopsLabels = this.generateQueryLabel(this.queriesResults.CLUSTER_IOPS_UTILIZATION);
    this.throughputLabels = this.generateQueryLabel(
      this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION
    );
    this.poolCapacityLabels = this.generateQueryLabel(
      this.queriesResults.POOL_CAPACITY_UTILIZATION,
      true
    );
    this.poolIOPSLabels = this.generateQueryLabel(this.queriesResults.POOL_IOPS_UTILIZATION, true);
    this.poolThroughputLabels = this.generateQueryLabel(
      this.queriesResults.POOL_THROUGHPUT_UTILIZATION,
      true
    );

    this.capacityValues = this.getQueryValues(this.queriesResults.CLUSTER_CAPACITY_UTILIZATION);
    this.iopsValues = this.getQueryValues(this.queriesResults.CLUSTER_IOPS_UTILIZATION);
    this.throughputValues = this.getQueryValues(this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION);
    this.poolCapacityValues = this.getQueryValues(this.queriesResults.POOL_CAPACITY_UTILIZATION);
    this.poolIOPSValues = this.getQueryValues(this.queriesResults.POOL_IOPS_UTILIZATION);
    this.poolThroughputValues = this.getQueryValues(
      this.queriesResults.POOL_THROUGHPUT_UTILIZATION
    );
  }

  findClusterData(metrics: any, clusterName: string) {
    const clusterMetrics = this.findCluster(metrics, clusterName);
    return parseInt(clusterMetrics?.value[1] || 0);
  }

  findCluster(metrics: any, clusterName: string) {
    return metrics.find((metric: any) => metric?.metric?.cluster === clusterName);
  }

  getVersion(fullVersion: string) {
    const version = fullVersion.replace('ceph version ', '').split(' ');
    return version[0] + ' ' + version.slice(2, version.length).join(' ');
  }

  generateQueryLabel(query: any, name = false, count = this.COUNT_OF_UTILIZATION_CHARTS) {
    let labels = [];
    for (let i = 0; i < count; i++) {
      let label = '';
      if (query[i]) {
        label = query[i]?.metric?.cluster;
        if (name) label = query[i]?.metric?.name + ' - ' + label;
      }
      labels.push(label);
    }
    // console.log(labels)
    return labels;
  }

  getQueryValues(query: any, count = this.COUNT_OF_UTILIZATION_CHARTS) {
    let values = [];
    for (let i = 0; i < count; i++) {
      if (query[i]) values.push(query[i]?.values);
    }
    return values;
  }
}
