import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiClient } from './api-client';

import { map } from 'rxjs/operators';
import { TopicModel } from '../models/topic.model';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient) {
    super();
  }

  listTopic(): Observable<TopicModel[]> {
    return this.http.get<TopicModel[]>(this.baseURL).pipe(
      map((resp) => {
        return resp;
      })
    );
  }

  get(name: string) {
    return this.http.get(`${this.baseURL}/${name}`);
  }
}
