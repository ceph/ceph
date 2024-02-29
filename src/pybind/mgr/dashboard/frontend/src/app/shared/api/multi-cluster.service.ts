import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subscription } from 'rxjs';
import { TimerService } from '../services/timer.service';
import { filter } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class MultiClusterService {
  TOKEN_CHECK_INTERVAL = 600000; // 10m interval
  private msSource = new BehaviorSubject<any>(null);
  msData$ = this.msSource.asObservable();
  private tokenStatusSource = new BehaviorSubject<any>(null);
  tokenStatusSource$ = this.tokenStatusSource.asObservable();
  constructor(private http: HttpClient, private timerService: TimerService) {}

  startPolling(): Subscription {
    return this.timerService
      .get(() => this.getCluster(), 5000)
      .subscribe(this.getClusterObserver());
  }

  startClusterTokenStatusPolling() {
    let clustersTokenMap = new Map<string, { token: string; user: string }>();
    const dataSubscription = this.subscribe((resp: any) => {
      const clustersConfig = resp['config'];
      const tempMap = new Map<string, { token: string; user: string }>();
      if (clustersConfig) {
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
          dataSubscription.unsubscribe();
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

  editCluster(url: any, clusterAlias: string, username: string) {
    return this.http.put('api/multi-cluster/edit_cluster', {
      url,
      cluster_alias: clusterAlias,
      username
    });
  }

  addCluster(
    url: any,
    clusterAlias: string,
    username: string,
    password: string,
    token = '',
    hub_url = '',
    clusterFsid = '',
    ssl = false,
    cert = '',
    ttl = ''
  ) {
    return this.http.post('api/multi-cluster/auth', {
      url,
      cluster_alias: clusterAlias,
      username,
      password,
      token,
      hub_url,
      cluster_fsid: clusterFsid,
      ssl_verify: ssl,
      ssl_certificate: cert,
      ttl: ttl
    });
  }

  reConnectCluster(
    url: any,
    username: string,
    password: string,
    token = '',
    ssl = false,
    cert = ''
  ) {
    return this.http.post('api/multi-cluster/reconnect_cluster', {
      url,
      username,
      password,
      token,
      ssl_verify: ssl,
      ssl_certificate: cert
    });
  }

  verifyConnection(
    url: string,
    username: string,
    password: string,
    token = '',
    ssl = false,
    cert = ''
  ): Observable<any> {
    return this.http.post('api/multi-cluster/verify_connection', {
      url: url,
      username: username,
      password: password,
      token: token,
      ssl_verify: ssl,
      ssl_certificate: cert
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
}
