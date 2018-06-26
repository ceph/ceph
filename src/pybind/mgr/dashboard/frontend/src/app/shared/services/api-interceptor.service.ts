import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import * as _ from 'lodash';
import { Observable, throwError as observableThrowError } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { NotificationType } from '../enum/notification-type.enum';
import { FinishedTask } from '../models/finished-task';
import { AuthStorageService } from './auth-storage.service';
import { NotificationService } from './notification.service';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class ApiInterceptorService implements HttpInterceptor {
  constructor(
    private router: Router,
    private authStorageService: AuthStorageService,
    public notificationService: NotificationService
  ) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).pipe(
      catchError((resp) => {
        if (resp instanceof HttpErrorResponse) {
          let showNotification = true;
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
              this.notificationService.notifyTask(finishedTask);
              showNotification = false;
              break;
            case 401:
              this.authStorageService.remove();
              this.router.navigate(['/login']);
              break;
            case 403:
              this.router.navigate(['/403']);
              break;
            case 404:
              this.router.navigate(['/404']);
              break;
          }

          let timeoutId;
          if (showNotification) {
            timeoutId = this.notificationService.show(
              NotificationType.error,
              resp.error.detail || '',
              `${resp.status} - ${resp.statusText}`
            );
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
}
