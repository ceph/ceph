import { HttpClient } from '@angular/common/http';
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
  prometheusConnectionError: any[] = [];

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

  startClusterTokenStatusPolling() {
    this.checkAndStartTimer();
  }

  private checkAndStartTimer() {
    this.checkTokenStatus().subscribe(this.getClusterTokenStatusObserver());
    this.timerService
      .get(() => this.checkTokenStatus(), this.TOKEN_CHECK_INTERVAL)
      .subscribe(this.getClusterTokenStatusObserver());
  }

  subscribeClusterTokenStatus(next: (data: any) => void, error?: (error: any) => void) {
    return this.tokenStatusSource$.pipe(filter((value) => !!value)).subscribe(next, error);
  }

  refresh(): Subscription {
    return this.getCluster().subscribe(this.getClusterObserver());
  }

  refreshTokenStatus() {
    return this.checkTokenStatus().subscribe(this.getClusterTokenStatusObserver());
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
    name: string,
    url: any,
    clusterAlias: string,
    username: string,
    verify = false,
    ssl_certificate = ''
  ) {
    return this.http.put('api/multi-cluster/edit_cluster', {
      name: name,
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
    ttl: number,
    cluster_token?: string
  ) {
    const requestBody: any = {
      url,
      username,
      password,
      ssl_verify: ssl,
      ssl_certificate: cert,
      ttl: ttl
    };

    if (cluster_token) {
      requestBody.cluster_token = cluster_token;
    }

    return this.http.put('api/multi-cluster/reconnect_cluster', requestBody);
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

  checkTokenStatus(): Observable<object> {
    return this.http.get<object>('api/multi-cluster/check_token_status');
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

  managePrometheusConnectionError(prometheusConnectionError?: any[]) {
    if (prometheusConnectionError !== undefined) {
      this.prometheusConnectionError = prometheusConnectionError;
    }
    return this.prometheusConnectionError;
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
