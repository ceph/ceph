import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb/cluster';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(`${this.baseURL}`);
  }

  create(request: any) {
    return this.http.post(`${this.baseURL}`, {request });
  }

  delete(name: string) {
    return this.http.delete(`${this.baseURL}/remove/${name}`, {
      observe: 'response'
    });
  }
}
