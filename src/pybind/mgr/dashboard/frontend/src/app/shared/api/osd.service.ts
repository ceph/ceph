import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class OsdService {
  private path = 'api/osd';

  constructor(private http: HttpClient) {}

  getList() {
    return this.http.get(`${this.path}`);
  }

  getDetails(id: number) {
    return this.http.get(`${this.path}/${id}`);
  }

  scrub(id, deep) {
    return this.http.post(`${this.path}/${id}/scrub?deep=${deep}`, null);
  }

  getFlags() {
    return this.http.get(`${this.path}/flags`);
  }

  updateFlags(flags: string[]) {
    return this.http.put(`${this.path}/flags`, { flags: flags });
  }

  markOut(id: number) {
    return this.http.post(`${this.path}/${id}/mark_out`, null);
  }

  markIn(id: number) {
    return this.http.post(`${this.path}/${id}/mark_in`, null);
  }

  markDown(id: number) {
    return this.http.post(`${this.path}/${id}/mark_down`, null);
  }

  reweight(id: number, weight: number) {
    return this.http.post(`${this.path}/${id}/reweight`, { weight: weight });
  }

  markLost(id: number) {
    return this.http.post(`${this.path}/${id}/mark_lost`, null);
  }

  remove(id: number) {
    return this.http.post(`${this.path}/${id}/remove`, null);
  }

  destroy(id: number) {
    return this.http.post(`${this.path}/${id}/destroy`, null);
  }

  safeToDestroy(id: number) {
    interface SafeToDestroyResponse {
      'safe-to-destroy': boolean;
      message?: string;
    }
    return this.http.get<SafeToDestroyResponse>(`${this.path}/${id}/safe_to_destroy`);
  }
}
