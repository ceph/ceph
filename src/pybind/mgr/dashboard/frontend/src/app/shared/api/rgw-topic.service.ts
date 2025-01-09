import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiClient } from './api-client';
import { TopicModel } from '~/app/ceph/rgw/rgw-topic-list/topic.model';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  constructor(private http: HttpClient) {
    super();
  }

  listTopic(uid?: string, tenant?: string): Observable<TopicModel[]> {
    let params = new HttpParams();
    if (uid) {
      params = params.append('uid', uid);
    } else if (tenant) {
      params = params.append('tenant', tenant);
    }

    return this.http
      .get<any>(this.baseURL, { params })
      .pipe(
        map((response) => {
          // Transform the response by flattening the 'dest' object
          if (response && response.topics) {
            return response.topics.map((topic: any) => ({
              owner: topic.owner,
              name: topic.name,
              arn: topic.arn,
              dest: {
                push_endpoint: topic.dest.push_endpoint,
                push_endpoint_args: topic.dest.push_endpoint_topic,
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
}
