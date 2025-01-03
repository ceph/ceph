import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class MgrModuleService {
  private url = 'api/mgr/module';

  constructor(private http: HttpClient) {}

  /**
   * Get the list of Ceph Mgr modules and their state (enabled/disabled).
   * @return {Observable<Object[]>}
   */
  list(): Observable<Object[]> {
    return this.http.get<Object[]>(`${this.url}`);
  }

  /**
   * Get the Ceph Mgr module configuration.
   * @param {string} module The name of the mgr module.
   * @return {Observable<Object>}
   */
  getConfig(module: string): Observable<Object> {
    return this.http.get(`${this.url}/${module}`);
  }

  /**
   * Update the Ceph Mgr module configuration.
   * @param {string} module The name of the mgr module.
   * @param {object} config The configuration.
   * @return {Observable<Object>}
   */
  updateConfig(module: string, config: object): Observable<Object> {
    return this.http.put(`${this.url}/${module}`, { config: config });
  }

  /**
   * Enable the Ceph Mgr module.
   * @param {string} module The name of the mgr module.
   */
  enable(module: string) {
    return this.http.post(`${this.url}/${module}/enable`, null);
  }

  /**
   * Disable the Ceph Mgr module.
   * @param {string} module The name of the mgr module.
   */
  disable(module: string) {
    return this.http.post(`${this.url}/${module}/disable`, null);
  }

  /**
   * Get the Ceph Mgr module options.
   * @param {string} module The name of the mgr module.
   * @return {Observable<Object>}
   */
  getOptions(module: string): Observable<Object> {
    return this.http.get(`${this.url}/${module}/options`);
  }
}
