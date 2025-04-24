import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { ApiClient } from './api-client';
import { CreateTopic, Topic } from '~/app/shared/models/topic.model';
import { catchError, mapTo } from 'rxjs/operators';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {
    super();
  }

  listTopic(): Observable<Topic> {
    return this.http.get<Topic>(this.baseURL);
  }

  getTopic(name: string) {
    return this.http.get<Topic>(`${this.baseURL}/${name}`);
  }

  create(createParam: CreateTopic) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(`${this.baseURL}`, createParam, { params: params });
    });
  }

  update(createParam: CreateTopic) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(`${this.baseURL}`, createParam, { params: params });
    });
  }
  delete(name: string) {
    return this.http.delete(`${this.baseURL}/${name}`, {
      observe: 'response'
    });
  }

  exists(name: string) {
    return this.getTopic(name).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }
}
