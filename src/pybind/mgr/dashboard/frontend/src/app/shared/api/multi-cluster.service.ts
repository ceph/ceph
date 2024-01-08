import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subscription } from 'rxjs';
import { TimerService } from '../services/timer.service';
import { filter } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class MultiClusterService {
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
    const clustersTokenMap = new Map<string, string>();
    this.subscribe((data: any) => {
      const config = data['config'];
      config?.forEach((cluster: any) => {
        cluster['token'] ? clustersTokenMap.set(cluster['name'], cluster['token']) : '';
      });
    })
    return this.timerService
      .get(() => this.checkTokenStatus(clustersTokenMap), 600000)
      .subscribe(this.getClusterTokenStatusObserver());
  }

  refresh(): Subscription {
    return this.getCluster().subscribe(this.getClusterObserver());
  }

  subscribe(next: (data: any) => void, error?: (error: any) => void) {
    return this.msData$.pipe(filter((value) => !!value)).subscribe(next, error);
  }

  subscribeClusterTokenStatus(next: (data: any) => void, error?: (error: any) => void) {
    return this.tokenStatusSource$.pipe(filter((value) => !!value)).subscribe(next, error);
  }

  setCluster(cluster: string) {
    return this.http.put('api/multicluster/set_config', { config: cluster });
  }

  getCluster() {
    return this.http.get('api/multicluster/get_config');
  }

  addCluster(
    url: any,
    name: string,
    helperText: string,
    username: string,
    password: string,
    token = '',
    origin = ''
  ) {
    return this.http.post('api/multicluster/auth', {
      url,
      helper_text: helperText,
      name,
      username,
      password,
      token,
      origin
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

  checkTokenStatus(clustersTokenMap: Map<string, string>): Observable<object> {
    let data = [...clustersTokenMap].map(([key, value]) => ({ key, value }));
    return this.http.put<object>('api/multicluster/check_token_status', { clustersTokenMap: data });
  }
}
