import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { ApiClient } from './api-client';
import { Topic, TopicRequest } from '~/app/shared/models/topic.model';
import { catchError, mapTo, shareReplay, tap } from 'rxjs/operators';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicService extends ApiClient {
  baseURL = 'api/rgw/topic';

  private topicCache = new Map<string, Observable<Topic>>();

  constructor(
    private http: HttpClient,
    private rgwDaemonService: RgwDaemonService
  ) {
    super();
  }

  listTopic(): Observable<Topic[]> {
    return this.http.get<Topic[]>(this.baseURL);
  }

  getTopic(key: string): Observable<Topic> {
    if (!this.topicCache.has(key)) {
      const request$ = this.http
        .get<Topic>(`${this.baseURL}/${encodeURIComponent(key)}`)
        .pipe(tap({ error: () => this.topicCache.delete(key) }), shareReplay(1));
      this.topicCache.set(key, request$);
    }
    return this.topicCache.get(key)!;
  }

  create(createParam: TopicRequest) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http
        .post(`${this.baseURL}`, createParam, { params: params })
        .pipe(tap(() => this.clearCache()));
    });
  }

  delete(key: string) {
    return this.http
      .delete(`${this.baseURL}/${key}`, {
        observe: 'response'
      })
      .pipe(tap(() => this.clearCache(key)));
  }

  exists(key: string): Observable<boolean> {
    const encodedKey = encodeURIComponent(`:${key}`);
    return this.getTopic(encodedKey).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }

  /**
   * Helper method to manually invalidate cache.
   * If a key is provided, clears just that topic. Otherwise, clears all.
   */
  private clearCache(key?: string): void {
    if (key) {
      this.topicCache.delete(key);
    } else {
      this.topicCache.clear();
    }
  }
}
