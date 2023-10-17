import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { tap } from 'rxjs/operators';
import _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class MulticlusterService {
  
  private selectedCluster = new BehaviorSubject<object>({});

  selectedCluster$ = this.selectedCluster.asObservable();

  private clusters = new BehaviorSubject<[]>([]);
  clusters$ = this.clusters.asObservable();

  

  

  constructor(private http: HttpClient) {}

  emitSelectionChanged(value: object) {
    this.selectedCluster.next(value);
  }

  getRemoteData() {
    
    return this.http.get(`api/multicluster/route`);
  }

  addRemoteCluster(apiToken: string, remoteClusterUrl: string) {
    console.log(apiToken, remoteClusterUrl);
    
    let requestBody = {
      apiToken: apiToken,
      remoteClusterUrl: remoteClusterUrl
    }
    return this.http.put(`api/health/add_remote_cluster`, requestBody);
  }

  setConfigs(value: object) {
    let requestBody = {
      remoteClusterUrl: value['remoteClusterUrl']
    }
    return this.http.put(`api/health/set_configs`, requestBody);
  }

  getRemoteClusterUrls() {
    return this.http.get(`api/health/get_remote_cluster_urls`).pipe(
      tap((clusters: any) => {
        this.clusters.next(clusters);
        const selectedCluster = this.selectedCluster.getValue();
        // Set or re-select the default daemon if the current one is not
        // in the list anymore.
        if (_.isEmpty(selectedCluster)) {
          this.emitSelectionChanged(clusters[0]);
        }
      })
    );
  }
}
