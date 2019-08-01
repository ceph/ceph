import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { Observable, throwError as observableThrowError } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { AuthStorageService } from './auth-storage.service';
import { NotificationService } from './notification.service';

@Injectable({
  providedIn: 'root'
})
export class ApiInterceptorService implements HttpInterceptor {
  private readonly backendUnreachableLimit = 4;
  private successfulResp = 0;

  constructor(
    private router: Router,
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    public notificationService: NotificationService
  ) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).pipe(
      tap(() => {
        this.removeNotification();
      }),
      catchError((resp) => {
        this.successfulResp = 0;
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
              this.router.navigate(['/403']);
              break;
            case 504:
              this.prepareNotification(
                resp,
                this.i18n('Ceph Dashboard backend service not reachable'),
                true
              );
              break;
            default:
              timeoutId = this.prepareNotification(resp);
          }

          /**
           * Decorated preventDefault method (in case error previously had
           * preventDefault method defined). If called, it will prevent a
           * notification to be shown.
           */
          resp['preventDefault'] = () => {
            this.notificationService.cancel(timeoutId);
          };

          /**
           * If called, it will prevent a notification for the specific status code.
           * @param {number} status The status code to be ignored.
           */
          resp['ignoreStatusCode'] = function(status: number) {
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

  private prepareNotification(resp, title?, isPermanent = false): number {
    return this.notificationService.show(() => {
      const errorCode = resp.status;
      if (!title) {
        title = `${resp.status} - ${resp.statusText}`;
      }
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
        title,
        message,
        undefined,
        resp['application'],
        errorCode,
        isPermanent
      );
    });
  }

  private removeNotification() {
    if (this.notificationService.getPermanentNotifications().length > 0) {
      // TODO: Find a better way to mute the notification
      if (this.successfulResp >= this.backendUnreachableLimit) {
        this.notificationService.removePermanentNotifications();
        this.successfulResp = 0;
      } else {
        this.successfulResp++;
      }
    }
  }
}
