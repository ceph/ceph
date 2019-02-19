import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class SettingRegistryService {
  constructor(private http: HttpClient) {}

  getSettingsList(): Observable<Object[]> {
    return this.http.get<Object[]>('ui-api/uisetting/get_setting');
  }

  updateSetting(name: string, config: Object): Observable<Object> {
    return this.http.put(`ui-api/uisetting/${name}`, { config: config });
  }
}
