import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class OsdService {
  private path = 'api/osd';

  constructor (private http: HttpClient) {}

  getList () {
    return this.http.get(`${this.path}`);
  }

  getDetails(id: number) {
    return this.http.get(`${this.path}/${id}`);
  }
}
