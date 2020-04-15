import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class LoggingService {
  constructor(private http: HttpClient) {}

  jsError(url: string, message: string, stack: any) {
    const request = {
      url: url,
      message: message,
      stack: stack
    };
    return this.http.post('ui-api/logging/js-error', request);
  }
}
