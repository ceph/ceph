import { HttpResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class CdTableServerSideService {

  constructor() { }

  static getCount(resp: HttpResponse<any>): number {
    return Number(resp.headers.get('X-Total-Count'))
  }
}
