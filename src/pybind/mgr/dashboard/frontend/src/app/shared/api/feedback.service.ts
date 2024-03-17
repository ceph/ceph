import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { CdHelperClass } from '../classes/cd-helper.class';

@Injectable({
  providedIn: 'root'
})
export class FeedbackService {
  constructor(private http: HttpClient) {}
  baseUIURL = 'api/feedback';

  isKeyExist() {
    return this.http.get('ui-api/feedback/api_key/exist');
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

  list() {
    return this.http.get('api/feedback', {
      headers: { Accept: CdHelperClass.cdVersionHeader('0', '1') }
    });
  }
}
