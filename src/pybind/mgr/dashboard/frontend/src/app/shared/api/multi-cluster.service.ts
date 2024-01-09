import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Subscription } from 'rxjs';
import { TimerService } from '../services/timer.service';
import { filter } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class MultiClusterService {
  private msSource = new BehaviorSubject<any>(null);
  msData$ = this.msSource.asObservable();
  constructor(private http: HttpClient, private timerService: TimerService) {}

  startPolling(): Subscription {
    return this.timerService
      .get(() => this.getCluster(), 5000)
      .subscribe(this.getClusterObserver());
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

  addCluster(
    url: any,
    clusterAlias: string,
    username: string,
    password: string,
    token = '',
    hub_url = ''
  ) {
    return this.http.post('api/multi-cluster/auth', {
      url,
      cluster_alias: clusterAlias,
      username,
      password,
      token,
      hub_url
    });
  }

  verifyConnection(url: string, username: string, password: string, token = '') {
    return this.http.post('api/multi-cluster/verify_connection', {
      url,
      username,
      password,
      token
    });
  }

  private getClusterObserver() {
    return (data: any) => {
      this.msSource.next(data);
    };
  }
}
