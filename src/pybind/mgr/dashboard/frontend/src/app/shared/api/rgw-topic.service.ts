import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { ApiClient } from './api-client';
import { TopicModel, ApiResponse } from '~/app/ceph/rgw/rgw-topic-list/topic.model';
import { catchError, map, mapTo } from 'rxjs/operators';
import { RgwDaemonService } from './rgw-daemon.service';
import { CreateTopicModel } from '~/app/ceph/rgw/rgw-create-topic-form/create-topic.model';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {
    super();
  }

  listTopic(): Observable<TopicModel[]> {
    return this.http.get<ApiResponse>(this.baseURL).pipe(
      map((response) => {
        // Transform the response by flattening the 'dest' object
        if (response && response.topics) {
          return response.topics.map((topic: TopicModel) => ({
            owner: topic.owner,
            name: topic.name,
            arn: topic.arn,
            dest: {
              push_endpoint: topic.dest.push_endpoint,
              push_endpoint_args: topic.dest.push_endpoint_args,
              push_endpoint_topic: topic.dest.push_endpoint_topic,
              stored_secret: topic.dest.stored_secret,
              persistent: topic.dest.persistent,
              persistent_queue: topic.dest.persistent_queue,
              time_to_live: topic.dest.time_to_live,
              max_retries: topic.dest.max_retries,
              retry_sleep_duration: topic.dest.retry_sleep_duration
            },
            opaqueData: topic.opaqueData,
            policy: topic.policy,
            subscribed_buckets: topic.subscribed_buckets
          }));
        } else {
          return []; // Return an empty array if there are no topics
        }
      })
    );
  }

  create(createParam: CreateTopicModel) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(`${this.baseURL}`, createParam, { params: params });
    });
  }

  update(createParam: CreateTopicModel) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(`${this.baseURL}`, createParam, { params: params });
    });
  }
  delete(name: string) {
    return this.http.delete(`${this.baseURL}/${name}`, {
      observe: 'response'
    });
  }

  validatetopicName(name: string) {
    return this.get(name).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }

  get(name: string) {
    return this.http.get(`${this.baseURL}/${name}`);
  }
}
