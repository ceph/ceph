import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiClient } from './api-client';
import { Topic } from '~/app/shared/models/topic.model';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient) {
    super();
  }

  listTopic(): Observable<Topic[]> {
    return this.http.get<Topic[]>(this.baseURL);
  }

  getTopic(name: string) {
    return this.http.get(`${this.baseURL}/${name}`);
  }
}
