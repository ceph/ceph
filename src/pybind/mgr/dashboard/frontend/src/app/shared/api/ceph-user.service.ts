import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class CephUserService {
  constructor(private http: HttpClient) {}

  export(entities: string[]) {
    return this.http.post('api/cluster/user/export', { entities: entities });
  }
}
