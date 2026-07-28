import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { throwError as observableThrowError } from 'rxjs';
import { catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class FeedbackService {
  constructor(private http: HttpClient) {}
  baseUIURL = 'api/feedback';

  isKeyExist() {
    return this.http.get('ui-api/feedback/api_key/exist').pipe(
      catchError((error) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableThrowError(() => error);
      })
    );
  }

  createIssue(
    project: string,
    tracker: string,
    subject: string,
    description: string,
    apiKey: string
  ) {
    return this.http.post(
      'api/feedback',
      {
        project: project,
        tracker: tracker,
        subject: subject,
        description: description,
        api_key: apiKey
      },
      {
        headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
      }
    );
  }
}
