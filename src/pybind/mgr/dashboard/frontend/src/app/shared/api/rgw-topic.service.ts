import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiClient } from './api-client';
import { TopicModel, ApiResponse } from '~/app/ceph/rgw/rgw-topic-list/topic.model';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient) {
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
  get(name: string) {
    return this.http.get(`${this.baseURL}/${name}`);
  }
}
