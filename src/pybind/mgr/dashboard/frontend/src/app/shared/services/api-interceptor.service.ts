import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import _ from 'lodash';
import { Observable, throwError as observableThrowError } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { AuthStorageService } from './auth-storage.service';
import { CookiesService } from './cookie.service';

export class CdHttpErrorResponse extends HttpErrorResponse {
  preventDefault: Function;
  ignoreStatusCode: Function;
}

@Injectable({
  providedIn: 'root'
})
export class ApiInterceptorService implements HttpInterceptor {
  constructor(
    private router: Router,
    public notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private cookieService: CookiesService
  ) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    const acceptHeader = request.headers.get('Accept');
    let reqWithVersion: HttpRequest<any>;

    const origin = window.location.origin;
    if (acceptHeader && acceptHeader.startsWith('application/vnd.ceph.api.v')) {
      reqWithVersion = request.clone();
    } else {
      reqWithVersion = request.clone({
        setHeaders: {
          Accept: CdHelperClass.cdVersionHeader('1', '0')
        }
      });
    }

    let apiUrl = localStorage.getItem('cluster_api_url');

    if (apiUrl && !apiUrl.endsWith('/')) {
      apiUrl += '/';
    }
    const currentRoute = this.router.url.split('?')[0];

    const ALWAYS_TO_HUB_APIs = [
      'api/auth/login',
      'api/auth/logout',
      'api/multi-cluster/set_config',
      'api/multi-cluster/get_config',
      'api/multi-cluster/auth'
    ];

    if (
      !currentRoute.includes('login') &&
      !ALWAYS_TO_HUB_APIs.includes(request.url) &&
      apiUrl &&
      !apiUrl.includes(origin)
    ) {
      const token = this.cookieService.getToken(localStorage.getItem('current_cluster_name'));
      reqWithVersion = reqWithVersion.clone({
        url: `${apiUrl}${reqWithVersion.url}`,
        setHeaders: {
          'Access-Control-Allow-Origin': origin,
          Authorization: `Bearer ${token}`
        }
      });
    }

    return next.handle(reqWithVersion).pipe(
      catchError((resp: CdHttpErrorResponse) => {
        if (resp instanceof HttpErrorResponse) {
          let timeoutId: number;
          switch (resp.status) {
            case 400:
              const finishedTask = new FinishedTask();

              const task = resp.error.task;
              if (_.isPlainObject(task)) {
                task.metadata.component = task.metadata.component || resp.error.component;

                finishedTask.name = task.name;
                finishedTask.metadata = task.metadata;
              } else {
                finishedTask.metadata = resp.error;
              }

              finishedTask.success = false;
              finishedTask.exception = resp.error;
              timeoutId = this.notificationService.notifyTask(finishedTask);
              break;
            case 401:
              this.authStorageService.remove();
              this.router.navigate(['/login']);
              break;
            case 403:
              this.router.navigate(['error'], {
                state: {
                  message: $localize`Sorry, you don’t have permission to view this page or resource.`,
                  header: $localize`Access Denied`,
                  icon: 'fa fa-lock',
                  source: 'forbidden'
                }
              });
              break;
            default:
              timeoutId = this.prepareNotification(resp);
          }

          /**
           * Decorated preventDefault method (in case error previously had
           * preventDefault method defined). If called, it will prevent a
           * notification to be shown.
           */
          resp.preventDefault = () => {
            this.notificationService.cancel(timeoutId);
          };

          /**
           * If called, it will prevent a notification for the specific status code.
           * @param {number} status The status code to be ignored.
           */
          resp.ignoreStatusCode = function (status: number) {
            if (this.status === status) {
              this.preventDefault();
            }
          };
        }
        // Return the error to the method that called it.
        return observableThrowError(resp);
      })
    );
  }

  private prepareNotification(resp: any): number {
    return this.notificationService.show(() => {
      let message = '';
      if (_.isPlainObject(resp.error) && _.isString(resp.error.detail)) {
        message = resp.error.detail; // Error was triggered by the backend.
      } else if (_.isString(resp.error)) {
        message = resp.error;
      } else if (_.isString(resp.message)) {
        message = resp.message;
      }
      return new CdNotificationConfig(
        NotificationType.error,
        `${resp.status} - ${resp.statusText}`,
        message,
        undefined,
        resp['application']
      );
    });
  }
}
