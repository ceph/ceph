import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class RbdMirroringService {
  constructor(private http: HttpClient) {}

  get() {
    return this.http.get('api/rbdmirror');
  }
}
