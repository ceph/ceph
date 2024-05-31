import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

const BASE_URL = 'api/nvmeof';

@Injectable({
  providedIn: 'root'
})
export class NvmeofService {
  constructor(private http: HttpClient) {}

  listGateways() {
    return this.http.get(`${BASE_URL}/gateway`);
  }
}
