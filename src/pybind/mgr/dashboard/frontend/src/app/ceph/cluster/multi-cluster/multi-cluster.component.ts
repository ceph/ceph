import { Component, Input, OnInit } from '@angular/core';
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
export class MultiClusterComponent implements OnInit{
  @Input() selectedValue: string;
  permission: Permission;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;
  icons = Icons;
  loading = true;

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
  };


  timerGetPrometheusDataSub: Subscription;

  addRemoteClusterAction: CdTableAction[];
  capacity: any = {};
  clusters: any;
  columns: Array<CdTableColumn> = [];
  alertColumns: Array<CdTableColumn> = [];
  tableData: any;
  clusterDataList: any;
  alerts: any;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    public modalService: ModalService,
    public multiClusterService: MultiClusterService,
    private prometheusService: PrometheusService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
  ) { 
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    const addRemoteAction: CdTableAction = {
      permission: 'read',
      icon: Icons.download,
      name: this.actionLabels.ADD + ' Remote Cluster',
      click: () => this.openRemoteClusterInfoModal()
    };
    this.addRemoteClusterAction = [addRemoteAction];
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
    this.columns = [
        { prop: 'cluster', name: $localize`Cluster Name`, flexGrow: 1 },
        { prop: 'status', name: $localize`Status`, flexGrow: 1, cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            1: { value: 'WARN', class: 'badge-warning' },
            0: { value: 'OK', class: 'badge-success' },
            2: { value: 'ERROR', class: 'badge-error' }
          }
        } },
        { prop: 'total_capacity', name: $localize`Total Capacity`, pipe: this.dimlessBinaryPipe, flexGrow: 1 },
        { prop: 'used_capacity', name: $localize`Used Capacity`, pipe: this.dimlessBinaryPipe, flexGrow: 1 },
        { prop: 'available_capacity', name: $localize`Available Capacity`, pipe: this.dimlessBinaryPipe, flexGrow: 1 },
        { prop: 'pools', name: $localize`Pools`, flexGrow: 1 },
        { prop: 'osds', name: $localize`OSDs`, flexGrow: 1 },
    ]
    this.alertColumns = [
        { prop: 'alertname', name: $localize`Name`, flexGrow: 1 },
        { prop: 'cluster', name: $localize`Cluster`, flexGrow: 1 },
        { prop: 'alertstate', name: $localize`State`, flexGrow: 1 },
        { prop: 'severity', name: $localize`Severity`, flexGrow: 1 },
      ]
    };


  getPrometheusData(selectedTime: any) {
    this.prometheusService.getMultiClusterQueriesData(selectedTime, queries, this.queriesResults)
      .subscribe((data: any) => {
        this.queriesResults = data;
        console.log(this.queriesResults);
        
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

    this.alerts.forEach((item: any) => {
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
      total_capacity: number;
      used_capacity: number;
      available_capacity: number;
      pools: number;
      osds: number;
    }
    
    const clusters: ClusterInfo[] = [];
    
    if (
      this.queriesResults.TOTAL_CAPACITY &&
      this.queriesResults.USED_CAPACITY &&
      this.queriesResults.POOLS &&
      this.queriesResults.OSDS &&
      this.queriesResults.HEALTH_STATUS &&
      this.queriesResults.ALERTS
    ) {
      this.queriesResults.TOTAL_CAPACITY.forEach((totalCapacityMetric: any) => {
        const clusterName = totalCapacityMetric.metric.cluster;
        const totalCapacity = parseInt(totalCapacityMetric.value[1]);
    
        const usedCapacityMetric = this.queriesResults.USED_CAPACITY.find(
          (metric: any) => metric.metric.cluster === clusterName
        );
        const usedCapacity = usedCapacityMetric ? parseInt(usedCapacityMetric.value[1]) : 0;
    
        const poolsMetric = this.queriesResults.POOLS.find(
          (poolMetric: any) => poolMetric.metric.cluster === clusterName
        );
        const pools = poolsMetric ? parseInt(poolsMetric.value[1]) : 0;
    
        const osdsMetric = this.queriesResults.OSDS.find(
          (osdMetric: any) => osdMetric.metric.cluster === clusterName
        );
        const osds = osdsMetric ? parseInt(osdsMetric.value[1]) : 0;

        const statusMetric = this.queriesResults.HEALTH_STATUS.find(
          (statusMetric: any) => statusMetric.metric.cluster === clusterName
        );
        const status = statusMetric ? parseInt(statusMetric.value[1]) : 0;

        const available_capacity = totalCapacity - usedCapacity;
    
        clusters.push({
          cluster: clusterName,
          status,
          total_capacity: totalCapacity,
          used_capacity: usedCapacity,
          available_capacity: available_capacity,
          pools,
          osds,
        });
      });
    }
    
    this.clusters = clusters;
    console.log(this.clusters);
    
  }
  

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, {
      size: 'lg'
    });
  }
}
