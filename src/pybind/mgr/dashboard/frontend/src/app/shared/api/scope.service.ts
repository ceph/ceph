import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ScopeService {
  constructor(private http: HttpClient) {}

  list() {
    return this.http.get('ui-api/scope');
  }
}
