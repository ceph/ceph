import {
  HttpErrorResponse,
  HttpEvent,
  HttpHandler,
  HttpInterceptor,
  HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import 'rxjs/add/observable/throw';
import 'rxjs/add/operator/catch';
import { Observable } from 'rxjs/Observable';

import { NotificationType } from '../enum/notification-type.enum';
import { AuthStorageService } from './auth-storage.service';
import { NotificationService } from './notification.service';

@Injectable()
export class ApiInterceptorService implements HttpInterceptor {

  constructor(private router: Router,
              private authStorageService: AuthStorageService,
              public notificationService: NotificationService) {}

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).catch(resp => {
      if (resp instanceof HttpErrorResponse) {
        let showNotification = true;
        switch (resp.status) {
          case 401:
            this.authStorageService.remove();
            this.router.navigate(['/login']);
            break;
          case 404:
            this.router.navigate(['/404']);
            break;
          case 409:
            showNotification = false;
            break;
        }
        if (showNotification) {
          this.notificationService.show(NotificationType.error,
            resp.error.detail || '',
            `${resp.status} - ${resp.statusText}`);
        }
      }
      // Return the error to the method that called it.
      return Observable.throw(resp);
    });
  }
}
