import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

export interface Motd {
  message: string;
  md5: string;
  severity: 'info' | 'warning' | 'danger';
  // The expiration date in ISO 8601. Does not expire if empty.
  expires: string;
}

@Injectable({
  providedIn: 'root'
})
export class MotdService {
  private url = 'ui-api/motd';

  constructor(private http: HttpClient) {}

  get(): Observable<Motd | null> {
    return this.http.get<Motd | null>(this.url);
  }
}
