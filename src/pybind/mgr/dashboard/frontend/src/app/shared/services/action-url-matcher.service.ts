import { Injectable } from '@angular/core';
import { URLVerbs } from '../constants/app.constants';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class ActionUrlMatcherService {
  constructor(private router: Router) {}

  match(path: string, verb: URLVerbs): boolean {
    return RegExp(`/.*${path}/${verb}/`, 'g').test(this.router.url);
  }
}
