import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { Subscription } from 'rxjs';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { MultiClusterPromqls as queries } from '~/app/shared/enum/dashboard-promqls.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { MultiClusterFormComponent } from './multi-cluster-form/multi-cluster-form.component';

@Component({
  selector: 'cd-multi-cluster',
  templateUrl: './multi-cluster.component.html',
  styleUrls: ['./multi-cluster.component.scss']
})
export class MultiClusterComponent implements OnInit {
  @Input() selectedValue: string;
  permission: Permission;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;
  icons = Icons;
  loading = true;
  @ViewChild('nameTpl', { static: true })
  nameTpl: any;
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
    CLUSTER_ALERTS: 0
  };

  timerGetPrometheusDataSub: Subscription;

  addRemoteClusterAction: CdTableAction[];
  capacity: any = {};
  clusters: any;
  dashboardClustersMap: Map<string, string> = new Map<string, string>();;
  columns: Array<CdTableColumn> = [];
  alertColumns: Array<CdTableColumn> = [];
  tableData: any;
  clusterDataList: any;
  alerts: any;
  private subs = new Subscription();
  clusterCapacityLabel1 = ''
  clusterCapacityLabel2 = ''
  clusterIopsLabel1 = ''
  clusterIopsLabel2 = ''
  clusterThroughputLabel1 = ''
  clusterThroughputLabel2 = ''
  clusterCapacityData1 = ''
  clusterCapacityData2 = ''
  clusterIopsData1 = ''
  clusterIopsData2 = ''
  clusterThroughputData1 = ''
  clusterThroughputData2 = ''
  poolCapacityLabel1 = ''
  poolCapacityLabel2 = ''
  poolIopsLabel1 = ''
  poolIopsLabel2 = ''
  poolThroughputLabel1 = ''
  poolThroughputLabel2 = ''
  poolCapacityData1 = ''
  poolCapacityData2 = ''
  poolIopsData1 = ''
  poolIopsData2 = ''
  poolThroughputData1 = ''
  poolThroughputData2 = ''

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    public modalService: ModalService,
    public multiClusterService: MultiClusterService,
    private prometheusService: PrometheusService,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    const addRemoteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.add,
      name: this.actionLabels.ADD + ' Cluster',
      click: () => this.openRemoteClusterInfoModal()
    };
    this.addRemoteClusterAction = [addRemoteAction];
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
    this.subs.add(
      this.multiClusterService.subscribe((resp: string) => {
        resp['config']?.forEach((config: any) => {
          this.dashboardClustersMap.set(config['url'], config['name']);
        });
      })
    );
    this.columns = [
      {
        prop: 'cluster',
        name: $localize`Cluster Name`,
        flexGrow: 1,
        cellTemplate: this.nameTpl
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
            2: { value: 'ERROR', class: 'badge-error' }
          }
        }
      },
      { prop: 'alert', name: $localize`Alerts`, flexGrow: 1 },
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
    this.alertColumns = [
      { prop: 'alertname', name: $localize`Name`, flexGrow: 1 },
      { prop: 'cluster', name: $localize`Cluster`, flexGrow: 1 },
      { prop: 'alertstate', name: $localize`State`, flexGrow: 1 },
      { prop: 'severity', name: $localize`Severity`, flexGrow: 1 }
    ];
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
      alertname: string;
      alertstate: string;
      severity: string;
      cluster: string;
    }

    const alerts: Alert[] = [];

    this.alerts?.forEach((item: any) => {
      const metric = item.metric;
      const alert: Alert = {
        alertname: metric.alertname,
        cluster: metric.cluster,
        alertstate: metric.alertstate,
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
    }

    const clusters: ClusterInfo[] = [];

    this.queriesResults.TOTAL_CAPACITY?.forEach((totalCapacityMetric: any) => {
      const clusterName = totalCapacityMetric.metric.cluster;
      const totalCapacity = parseInt(totalCapacityMetric.value[1]);
      const usedCapacity = this.findClusterData(this.queriesResults?.USED_CAPACITY, clusterName)
      const pools = this.findClusterData(this.queriesResults?.POOLS, clusterName);
      const hosts = this.findClusterData(this.queriesResults?.HOSTS, clusterName);
      const alert = this.findClusterData(this.queriesResults?.CLUSTER_ALERTS, clusterName);
      const osds =  this.findClusterData(this.queriesResults?.OSDS, clusterName);
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
        hosts
      });
    });

    this.clusters = clusters;
    this.clusterCapacityLabel1 = this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[0] ? this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[0].metric.cluster : '';
    this.clusterCapacityLabel2 = this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[1] ? this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[1].metric.cluster : '';
    this.clusterCapacityData1 = this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[0] ? this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[0].values : '';
    this.clusterCapacityData2 = this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[1] ? this.queriesResults.CLUSTER_CAPACITY_UTILIZATION[1].values : '';
    this.clusterIopsLabel1 = this.queriesResults.CLUSTER_IOPS_UTILIZATION[0] ? this.queriesResults.CLUSTER_IOPS_UTILIZATION[0].metric.cluster : '';
    this.clusterIopsLabel2 = this.queriesResults.CLUSTER_IOPS_UTILIZATION[1] ? this.queriesResults.CLUSTER_IOPS_UTILIZATION[1].metric.cluster : '';
    this.clusterIopsData1 = this.queriesResults.CLUSTER_IOPS_UTILIZATION[0] ? this.queriesResults.CLUSTER_IOPS_UTILIZATION[0].values : '';
    this.clusterIopsData2 = this.queriesResults.CLUSTER_IOPS_UTILIZATION[1] ? this.queriesResults.CLUSTER_IOPS_UTILIZATION[1].values : '';
    this.clusterThroughputLabel1 = this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[0] ? this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[0].metric.cluster : '';
    this.clusterThroughputLabel2 = this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[1] ? this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[1].metric.cluster : '';
    this.clusterThroughputData1 = this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[0] ? this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[0].values : '';
    this.clusterThroughputData2 = this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[1] ? this.queriesResults.CLUSTER_THROUGHPUT_UTILIZATION[1].values : '';
    this.poolCapacityLabel1 = this.queriesResults.POOL_CAPACITY_UTILIZATION[0] ? this.queriesResults.POOL_CAPACITY_UTILIZATION[0].metric.name + ' - ' + this.queriesResults.POOL_CAPACITY_UTILIZATION[0].metric.cluster: '';
    this.poolCapacityLabel2 = this.queriesResults.POOL_CAPACITY_UTILIZATION[1] ? this.queriesResults.POOL_CAPACITY_UTILIZATION[1].metric.name + ' - ' + this.queriesResults.POOL_CAPACITY_UTILIZATION[1].metric.cluster: '';
    this.poolCapacityData1 = this.queriesResults.POOL_CAPACITY_UTILIZATION[0] ? this.queriesResults.POOL_CAPACITY_UTILIZATION[0].values : '';
    this.poolCapacityData2 = this.queriesResults.POOL_CAPACITY_UTILIZATION[1] ? this.queriesResults.POOL_CAPACITY_UTILIZATION[1].values : '';
    this.poolIopsLabel1 = this.queriesResults.POOL_IOPS_UTILIZATION[0] ? this.queriesResults.POOL_IOPS_UTILIZATION[0].metric.name + ' - ' + this.queriesResults.POOL_IOPS_UTILIZATION[0].metric.cluster : '';
    this.poolIopsLabel2 = this.queriesResults.POOL_IOPS_UTILIZATION[1] ? this.queriesResults.POOL_IOPS_UTILIZATION[1].metric.name + ' - ' + this.queriesResults.POOL_IOPS_UTILIZATION[1].metric.cluster : '';
    this.poolIopsData1 = this.queriesResults.POOL_IOPS_UTILIZATION[0] ? this.queriesResults.POOL_IOPS_UTILIZATION[0].values : '';
    this.poolIopsData2 = this.queriesResults.POOL_IOPS_UTILIZATION[1] ? this.queriesResults.POOL_IOPS_UTILIZATION[1].values : '';
    this.poolThroughputLabel1 = this.queriesResults.POOL_THROUGHPUT_UTILIZATION[0] ? this.queriesResults.POOL_THROUGHPUT_UTILIZATION[0].metric.name + ' - ' + this.queriesResults.POOL_THROUGHPUT_UTILIZATION[0].metric.cluster : '';
    this.poolThroughputLabel2 = this.queriesResults.POOL_THROUGHPUT_UTILIZATION[1] ? this.queriesResults.POOL_THROUGHPUT_UTILIZATION[1].metric.name + ' - ' + this.queriesResults.POOL_THROUGHPUT_UTILIZATION[1].metric.cluster : '';
    this.poolThroughputData1 = this.queriesResults.POOL_THROUGHPUT_UTILIZATION[0] ? this.queriesResults.POOL_THROUGHPUT_UTILIZATION[0].values : '';
    this.poolThroughputData2 = this.queriesResults.POOL_THROUGHPUT_UTILIZATION[1] ? this.queriesResults.POOL_THROUGHPUT_UTILIZATION[1].values : ''; 
  }

  findClusterData(metrics: any, clusterName: string) {
    const clusterMetrics = metrics.find(
        (metric: any) => metric.metric.cluster === clusterName
      );
    return parseInt(clusterMetrics?.value[1])
  }

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, {
      size: 'lg'
    });
  }
}
