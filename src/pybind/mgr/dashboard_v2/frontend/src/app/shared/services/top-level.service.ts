import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Subject';

import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class TopLevelService {
  // Observable sources
  private topLevelDataSource = new Subject();

  // Observable streams
  topLevelData$ = this.topLevelDataSource.asObservable();

  constructor(private http: HttpClient, private authStorageService: AuthStorageService) {
    this.refresh();
  }

  refresh() {
    if (this.authStorageService.isLoggedIn()) {
      this.http.get('/api/dashboard/toplevel').subscribe(data => {
        this.topLevelDataSource.next(data);
      });
    }

    setTimeout(() => {
      this.refresh();
    }, 5000);
  }
}
