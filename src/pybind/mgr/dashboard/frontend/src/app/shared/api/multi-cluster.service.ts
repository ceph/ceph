import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subscription } from 'rxjs';
import { TimerService } from '../services/timer.service';
import { filter, first } from 'rxjs/operators';
import { SummaryService } from '../services/summary.service';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class MultiClusterService {
  TOKEN_CHECK_INTERVAL = 600000; // 10m interval
  private msSource = new BehaviorSubject<any>(null);
  msData$ = this.msSource.asObservable();
  private tokenStatusSource = new BehaviorSubject<any>(null);
  tokenStatusSource$ = this.tokenStatusSource.asObservable();
  showDeletionMessage = false;
  isClusterAddedFlag = false;
  constructor(
    private http: HttpClient,
    private timerService: TimerService,
    private summaryService: SummaryService,
    private router: Router
  ) {}

  startPolling(): Subscription {
    return this.timerService
      .get(() => this.getCluster(), 5000)
      .subscribe(this.getClusterObserver());
  }

  getTempMap(clustersConfig: any) {
    const tempMap = new Map<string, { token: string; user: string }>();
    Object.keys(clustersConfig).forEach((clusterKey: string) => {
      const clusterDetailsList = clustersConfig[clusterKey];
      clusterDetailsList.forEach((clusterDetails: any) => {
        if (clusterDetails['token'] && clusterDetails['name'] && clusterDetails['user']) {
          tempMap.set(clusterDetails['name'], {
            token: clusterDetails['token'],
            user: clusterDetails['user']
          });
        }
      });
    });
    return tempMap;
  }

  startClusterTokenStatusPolling() {
    let clustersTokenMap = new Map<string, { token: string; user: string }>();
    const dataSubscription = this.subscribeOnce((resp: any) => {
      const clustersConfig = resp['config'];
      let tempMap = new Map<string, { token: string; user: string }>();
      if (clustersConfig) {
        tempMap = this.getTempMap(clustersConfig);
        Object.keys(clustersConfig).forEach((clusterKey: string) => {
          const clusterDetailsList = clustersConfig[clusterKey];
          clusterDetailsList.forEach((clusterDetails: any) => {
            if (clusterDetails['token'] && clusterDetails['name'] && clusterDetails['user']) {
              tempMap.set(clusterDetails['name'], {
                token: clusterDetails['token'],
                user: clusterDetails['user']
              });
            }
          });
        });

        if (tempMap.size > 0) {
          clustersTokenMap = tempMap;
          if (dataSubscription) {
            dataSubscription.unsubscribe();
          }
          this.checkAndStartTimer(clustersTokenMap);
        }
      }
    });
  }

  private checkAndStartTimer(clustersTokenMap: Map<string, { token: string; user: string }>) {
    this.checkTokenStatus(clustersTokenMap).subscribe(this.getClusterTokenStatusObserver());
    this.timerService
      .get(() => this.checkTokenStatus(clustersTokenMap), this.TOKEN_CHECK_INTERVAL)
      .subscribe(this.getClusterTokenStatusObserver());
  }

  subscribeClusterTokenStatus(next: (data: any) => void, error?: (error: any) => void) {
    return this.tokenStatusSource$.pipe(filter((value) => !!value)).subscribe(next, error);
  }

  refresh(): Subscription {
    return this.getCluster().subscribe(this.getClusterObserver());
  }

  refreshTokenStatus() {
    this.subscribeOnce((resp: any) => {
      const clustersConfig = resp['config'];
      let tempMap = this.getTempMap(clustersConfig);
      return this.checkTokenStatus(tempMap).subscribe(this.getClusterTokenStatusObserver());
    });
  }

  subscribeOnce(next: (data: any) => void, error?: (error: any) => void) {
    return this.msData$
      .pipe(
        filter((value) => !!value),
        first()
      )
      .subscribe(next, error);
  }

  subscribe(next: (data: any) => void, error?: (error: any) => void) {
    return this.msData$.pipe(filter((value) => !!value)).subscribe(next, error);
  }

  setCluster(cluster: object) {
    return this.http.put('api/multi-cluster/set_config', { config: cluster });
  }

  getCluster() {
    return this.http.get('api/multi-cluster/get_config');
  }

  deleteCluster(clusterName: string, clusterUser: string): Observable<any> {
    return this.http.delete(`api/multi-cluster/delete_cluster/${clusterName}/${clusterUser}`);
  }

  editCluster(
    url: any,
    clusterAlias: string,
    username: string,
    verify = false,
    ssl_certificate = ''
  ) {
    return this.http.put('api/multi-cluster/edit_cluster', {
      url,
      cluster_alias: clusterAlias,
      username: username,
      verify: verify,
      ssl_certificate: ssl_certificate
    });
  }

  addCluster(
    url: any,
    clusterAlias: string,
    username: string,
    password: string,
    hub_url = '',
    ssl = false,
    cert = '',
    ttl: number
  ) {
    return this.http.post('api/multi-cluster/auth', {
      url,
      cluster_alias: clusterAlias,
      username,
      password,
      hub_url,
      ssl_verify: ssl,
      ssl_certificate: cert,
      ttl: ttl
    });
  }

  reConnectCluster(
    url: any,
    username: string,
    password: string,
    ssl = false,
    cert = '',
    ttl: number
  ) {
    return this.http.put('api/multi-cluster/reconnect_cluster', {
      url,
      username,
      password,
      ssl_verify: ssl,
      ssl_certificate: cert,
      ttl: ttl
    });
  }

  private getClusterObserver() {
    return (data: any) => {
      this.msSource.next(data);
    };
  }

  private getClusterTokenStatusObserver() {
    return (data: any) => {
      this.tokenStatusSource.next(data);
    };
  }

  checkTokenStatus(
    clustersTokenMap: Map<string, { token: string; user: string }>
  ): Observable<object> {
    let data = [...clustersTokenMap].map(([key, { token, user }]) => ({ name: key, token, user }));

    let params = new HttpParams();
    params = params.set('clustersTokenMap', JSON.stringify(data));

    return this.http.get<object>('api/multi-cluster/check_token_status', { params });
  }

  showPrometheusDelayMessage(showDeletionMessage?: boolean) {
    if (showDeletionMessage !== undefined) {
      this.showDeletionMessage = showDeletionMessage;
    }
    return this.showDeletionMessage;
  }

  isClusterAdded(isClusterAddedFlag?: boolean) {
    if (isClusterAddedFlag !== undefined) {
      this.isClusterAddedFlag = isClusterAddedFlag;
    }
    return this.isClusterAddedFlag;
  }

  refreshMultiCluster(currentRoute: string) {
    this.refresh();
    this.refreshTokenStatus();
    this.summaryService.refresh();
    if (currentRoute.includes('dashboard')) {
      this.router.navigateByUrl('/pool', { skipLocationChange: true }).then(() => {
        this.router.navigate([currentRoute]);
      });
    } else {
      this.router.navigateByUrl('/', { skipLocationChange: true }).then(() => {
        this.router.navigate([currentRoute]);
      });
    }
  }
}
