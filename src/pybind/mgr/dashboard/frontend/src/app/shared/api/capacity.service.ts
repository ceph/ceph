import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ApiClient } from '~/app/shared/api/api-client';

@Injectable({
  providedIn: 'root'
})
export class CapacityService extends ApiClient{

  constructor(private http: HttpClient) {
    super();
  }

  list() {
    return this.http.get(`api/capacity`, {
      observe: "body",
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
    });
  }
}
