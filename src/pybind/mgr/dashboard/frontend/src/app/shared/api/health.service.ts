import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class HealthService {
  constructor(private http: HttpClient) {}

  getFullHealth() {
    return this.http.get('api/health/full');
  }

  getMinimalHealth() {
    return this.http.get('api/health/minimal');
  }
}
