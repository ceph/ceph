import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';

import { Subject } from 'rxjs/Subject';

import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class SummaryService {
  // Observable sources
  private summaryDataSource = new Subject();

  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(
    private http: HttpClient,
    private authStorageService: AuthStorageService,
    private ngZone: NgZone
  ) {
    this.refresh();
  }

  refresh() {
    if (this.authStorageService.isLoggedIn()) {
      this.http.get('api/summary').subscribe(data => {
        this.summaryDataSource.next(data);
      });
    }

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => {
        this.ngZone.run(() => {
          this.refresh();
        });
      }, 5000);
    });
  }

  get() {
    return this.http.get('api/summary').toPromise().then((resp: any) => {
      return resp;
    });
  }
}
