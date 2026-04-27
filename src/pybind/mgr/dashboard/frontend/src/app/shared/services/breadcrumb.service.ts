import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

import { IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class BreadcrumbService {
  private tabCrumbSubject = new BehaviorSubject<IBreadcrumb>(null);
  tabCrumb$ = this.tabCrumbSubject.asObservable();

  setTabCrumb(text: string, path: string = null): void {
    this.tabCrumbSubject.next({ text, path });
  }

  clearTabCrumb(): void {
    this.tabCrumbSubject.next(null);
  }
}
