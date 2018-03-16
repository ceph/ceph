import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Subject';

import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class SummaryService {
  // Observable sources
  private summaryDataSource = new Subject();

  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(private http: HttpClient, private authStorageService: AuthStorageService) {
    this.refresh();
  }

  refresh() {
    if (this.authStorageService.isLoggedIn()) {
      this.http.get('api/summary').subscribe(data => {
        this.summaryDataSource.next(data);
      });
    }

    setTimeout(() => {
      this.refresh();
    }, 5000);
  }
}
