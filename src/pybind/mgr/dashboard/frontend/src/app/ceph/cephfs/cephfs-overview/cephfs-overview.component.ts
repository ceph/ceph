import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { Component, OnInit } from '@angular/core';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { Observable, forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';

@Component({
  selector: 'cd-cephfs-overview',
  templateUrl: './cephfs-overview.component.html',
  styleUrls: ['./cephfs-overview.component.scss'],
  providers: [DimlessBinaryPipe],
  standalone: false
})
export class CephfsOverviewComponent implements OnInit {
  isLoading = true;

  // KPIs
  fileSystemCount = 0;
  totalClientConnections = 0;
  activeMdsCount = 0;
  standbyMdsCount = 0;
  totalSubvolumes = 0;
  totalSubvolumeGroups = 0;
  
  // Health
  healthStatus = 'OK';
  healthColor: 'success' | 'warning' | 'danger' | 'info' = 'success';
  healthIssues: { type: string; name: string; state: string; color: string }[] = [];

  // Capacity
  totalCapacity = 0;
  totalUsed = 0;
  totalAvailable = 0;
  totalDataCapacity = 0;
  totalDataUsed = 0;
  totalMetadataCapacity = 0;
  totalMetadataUsed = 0;

  capacityChartData: any;
  capacityChartOptions: any;

  // Metrics
  clientRequestsQueries: any;
  clientRequestsChartData: any;
  
  throughputQueries: any;
  throughputChartData: any;

  // Gateways & Protocols
  nfsClusterCount = 0;
  nfsExportCount = 0;
  smbClusterCount = 0;
  smbShareCount = 0;
  // Mirroring
  mirrorDaemonCount = 0;
  mirrorPeerCount = 0;
  mirrorHealthStatus: string = 'OK';
  mirrorHealthColor: 'success' | 'warning' | 'danger' | 'info' = 'success';

  constructor(
    private cephfsService: CephfsService,
    private cephfsSubvolumeService: CephfsSubvolumeService,
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private poolService: PoolService,
    private prometheusService: PrometheusService,
    private formatter: FormatterService,
    private performanceCardService: PerformanceCardService,
    private nfsService: NfsService,
    private smbService: SmbService
  ) {}

  ngOnInit(): void {
    this.setupCharts();
    this.loadCephfsData();
  }

  private setupCharts() {
    this.capacityChartOptions = {
      resizable: true,
      legend: {
        position: 'bottom'
      },
      donut: {
        center: {
          label: $localize`Raw Used`
        },
        alignment: 'center'
      }
    };

    // Prometheus queries for performance
    this.clientRequestsQueries = {
      'Client Reads': 'sum(rate(ceph_mds_request{}[1m]))',
      'Client Replies': 'sum(rate(ceph_mds_reply{}[1m]))'
    };
    
    this.throughputQueries = {
      'Metadata Read (B/s)': 'sum(rate(ceph_mds_server_handle_client_request{}[1m]))',
      'Metadata Write (B/s)': 'sum(rate(ceph_mds_server_handle_client_session{}[1m]))'
    };
  }

  loadPerformanceData(selectedTime?: { start: number; end: number; step: number }): void {
    const time = selectedTime || {
      start: Math.floor(Date.now() / 1000) - 3600,
      end: Math.floor(Date.now() / 1000),
      step: 14
    };

    this.prometheusService
      .getRangeQueriesData(time, { 
        CLIENT_READS: this.clientRequestsQueries['Client Reads'], 
        CLIENT_REPLIES: this.clientRequestsQueries['Client Replies'] 
      }, true)
      .subscribe((results) => {
        const reads = this.performanceCardService.toSeries(results?.CLIENT_READS || [], 'Client Reads');
        const replies = this.performanceCardService.toSeries(results?.CLIENT_REPLIES || [], 'Client Replies');
        this.clientRequestsChartData = this.performanceCardService.mergeSeries(reads, replies);
        
        if (!this.clientRequestsChartData.length) {
          this.clientRequestsChartData = [{ timestamp: new Date(), values: { 'Client Reads': 0, 'Client Replies': 0 } }];
        }
      });

    this.prometheusService
      .getRangeQueriesData(time, { 
        META_READS: this.throughputQueries['Metadata Read (B/s)'], 
        META_WRITES: this.throughputQueries['Metadata Write (B/s)'] 
      }, true)
      .subscribe((results) => {
        const reads = this.performanceCardService.toSeries(results?.META_READS || [], 'Metadata Read');
        const writes = this.performanceCardService.toSeries(results?.META_WRITES || [], 'Metadata Write');
        this.throughputChartData = this.performanceCardService.mergeSeries(reads, writes);
        
        if (!this.throughputChartData.length) {
          this.throughputChartData = [{ timestamp: new Date(), values: { 'Metadata Read': 0, 'Metadata Write': 0 } }];
        }
      });
  }

  private loadCephfsData() {
    this.isLoading = true;

    forkJoin({
      cephfsList: this.cephfsService.list().pipe(catchError(() => of([]))),
      pools: this.poolService.getList().pipe(catchError(() => of([]))),
      nfsClusters: this.nfsService.nfsClusterList().pipe(catchError(() => of([]))),
      nfsExports: this.nfsService.list().pipe(catchError(() => of([]))),
      smbClusters: this.smbService.listClusters().pipe(catchError(() => of([]))),
      mirrorDaemons: this.cephfsService.listDaemonStatus().pipe(catchError(() => of([])))
    }).subscribe(({ cephfsList, pools, nfsClusters, nfsExports, smbClusters, mirrorDaemons }) => {
      this.processCephfsData(cephfsList as any[], pools as any[]);
      
      this.nfsClusterCount = (nfsClusters as any[]).length;
      this.nfsExportCount = (nfsExports as any[]).length;
      this.smbClusterCount = (smbClusters as any[]).length;
      
      this.mirrorDaemonCount = (mirrorDaemons as any[]).length;
      let totalPeers = 0;
      let mirrorErrors = 0;
      if (mirrorDaemons && (mirrorDaemons as any[]).length) {
        (mirrorDaemons as any[]).forEach(d => {
          if (d.filesystems) {
            d.filesystems.forEach((fs: any) => {
              if (fs.peers) {
                totalPeers += fs.peers.length;
              }
              // Check mirroring health via peer stats
              if (fs.peers) {
                fs.peers.forEach((peer: any) => {
                  if (peer.stats && peer.stats.failure_count > 0) {
                    mirrorErrors++;
                  }
                });
              }
            });
          }
        });
      }
      this.mirrorPeerCount = totalPeers;
      if (mirrorErrors > 0) {
        this.mirrorHealthStatus = `Error (${mirrorErrors})`;
        this.mirrorHealthColor = 'danger';
      } else {
        this.mirrorHealthStatus = 'OK';
        this.mirrorHealthColor = 'success';
      }

      const clientReqs: Observable<any>[] = [];
      const subvolumeReqs: Observable<any>[] = [];
      const subvolGroupReqs: Observable<any>[] = [];
      const smbShareReqs: Observable<any>[] = [];

      (cephfsList as any[]).forEach(fs => {
        clientReqs.push(this.cephfsService.getClients(fs.id).pipe(catchError(() => of({ status: 1, data: [] }))));
        
        const fsName = fs.mdsmap.fs_name;
        const groupsReq = this.cephfsSubvolumeGroupService.get(fsName, false).pipe(catchError(() => of([])));
        subvolGroupReqs.push(groupsReq);

        const allSubvolsReq = groupsReq.pipe(
          switchMap(groups => {
            const svReqs: Observable<any>[] = [];
            // Always fetch default group
            svReqs.push(this.cephfsSubvolumeService.get(fsName, '', false).pipe(catchError(() => of([]))));
            if (groups && groups.length) {
              groups.forEach((g: any) => {
                if (g.name !== '_nogroup') { // Avoid duplicate if backend happens to return it
                  svReqs.push(this.cephfsSubvolumeService.get(fsName, g.name, false).pipe(catchError(() => of([]))));
                }
              });
            }
            return forkJoin(svReqs);
          }),
          map(results => {
            let total = 0;
            results.forEach((res: any) => {
              if (res && res.length) total += res.length;
            });
            return total;
          })
        );
        subvolumeReqs.push(allSubvolsReq);
      });

      (smbClusters as any[]).forEach(cluster => {
        smbShareReqs.push(this.smbService.listShares(cluster.cluster_id).pipe(catchError(() => of([]))));
      });

      if ((cephfsList as any[]).length > 0 || (smbClusters as any[]).length > 0) {
        forkJoin({
          clients: clientReqs.length ? forkJoin(clientReqs) : of([]),
          subvolumes: subvolumeReqs.length ? forkJoin(subvolumeReqs) : of([]),
          subvolGroups: subvolGroupReqs.length ? forkJoin(subvolGroupReqs) : of([]),
          smbShares: smbShareReqs.length ? forkJoin(smbShareReqs) : of([])
        }).subscribe(({ clients, subvolumes, subvolGroups, smbShares }) => {
          let totalClients = 0;
          clients.forEach(res => {
            if (res && res.data) {
              totalClients += res.data.length;
            }
          });
          this.totalClientConnections = totalClients;

          let totalSubvols = 0;
          subvolumes.forEach(res => {
            if (typeof res === 'number') {
              totalSubvols += res;
            } else if (res && res.length) {
              totalSubvols += res.length;
            }
          });
          this.totalSubvolumes = totalSubvols;

          let totalGroups = 0;
          subvolGroups.forEach(res => {
            if (res && res.length) {
              totalGroups += res.length;
            }
          });
          this.totalSubvolumeGroups = totalGroups;

          let totalSmbShares = 0;
          smbShares.forEach(res => {
            if (res && res.length) {
              totalSmbShares += res.length;
            }
          });
          this.smbShareCount = totalSmbShares;

          this.isLoading = false;
        });
      } else {
        this.isLoading = false;
      }
    });
  }

  private processCephfsData(filesystems: any[], pools: any[]) {
    this.fileSystemCount = filesystems.length;
    
    let activeMds = 0;
    let standbyMds = 0;
    
    const dataPoolIds = new Set<number>();
    const metadataPoolIds = new Set<number>();

    let errorCount = 0;
    let warningCount = 0;

    filesystems.forEach(fs => {
      if (fs.mdsmap && fs.mdsmap.info) {
        Object.values(fs.mdsmap.info).forEach((daemon: any) => {
          if (daemon.state === 'up:active') activeMds++;
          else standbyMds++;
        });
      }
      
      if (fs.mdsmap.data_pools) {
        fs.mdsmap.data_pools.forEach((pid: any) => dataPoolIds.add(Number(pid)));
      }
      if (fs.mdsmap.metadata_pool) {
        metadataPoolIds.add(Number(fs.mdsmap.metadata_pool));
      }
    });

    this.activeMdsCount = activeMds;
    this.standbyMdsCount = standbyMds;

    // Calculate capacity
    pools.forEach(pool => {
      const poolId = Number(pool.pool);
      const isData = dataPoolIds.has(poolId);
      const isMeta = metadataPoolIds.has(poolId);
      
      if (isData || isMeta) {
        const stats = pool.stats || {};
        const bytesUsed = stats.bytes_used?.latest || 0;
        const maxAvail = stats.max_avail?.latest || 0;
        
        if (isData) {
          this.totalDataUsed += bytesUsed;
          this.totalDataCapacity += (bytesUsed + maxAvail);
        }
        if (isMeta) {
          this.totalMetadataUsed += bytesUsed;
          this.totalMetadataCapacity += (bytesUsed + maxAvail);
        }

        this.totalUsed += bytesUsed;
        this.totalCapacity += (bytesUsed + maxAvail);
      }
    });

    this.totalAvailable = this.totalCapacity - this.totalUsed;

    const usedPercentage =
      this.totalCapacity > 0 ? (this.totalUsed / this.totalCapacity) * 100 : 0;
    const formattedUsedPercentage = this.formatter.format_number(usedPercentage, 100, ['%'], 1);
    
    this.capacityChartOptions.donut.center.label = `${formattedUsedPercentage} \n Raw Used`;

    this.capacityChartData = [
      {
        group: 'Used',
        value: this.totalUsed
      },
      {
        group: 'Available',
        value: this.totalCapacity > 0 ? this.totalAvailable : 1
      }
    ];

    if (errorCount > 0) {
      this.healthStatus = `Error (${errorCount})`;
      this.healthColor = 'danger';
    } else if (warningCount > 0) {
      this.healthStatus = `Warning (${warningCount})`;
      this.healthColor = 'warning';
    } else {
      this.healthStatus = 'OK';
      this.healthColor = 'success';
    }
  }

  get usagePercentNumber(): number {
    if (this.totalCapacity > 0) {
      return (this.totalUsed / this.totalCapacity) * 100;
    }
    return 0;
  }

  percentageFormatter = (value: number) => {
    if (value > 0 && value < 0.1) {
      return '< 0.1%';
    }
    return `${value.toFixed(1)}%`;
  };

  get overviewFields() {
    return [
      { label: $localize`File Systems`, value: this.fileSystemCount, type: 'text', routerLink: '/cephfs/fs' },
      { label: $localize`Subvolumes`, value: this.totalSubvolumes, type: 'text' },
      { label: $localize`Subvolume Groups`, value: this.totalSubvolumeGroups, type: 'text' },
      { label: $localize`Client Connections`, value: this.totalClientConnections, type: 'text' },
      { label: $localize`Active MDS Daemons`, value: this.activeMdsCount, type: 'text' },
      {
        label: $localize`Mirroring Health`,
        value: this.mirrorHealthStatus,
        type: 'status',
        status: this.mirrorHealthColor
      }
    ];
  }
}
