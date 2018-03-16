import {
  HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';
import 'rxjs/add/observable/throw';
import 'rxjs/add/operator/catch';
import { Observable } from 'rxjs/Observable';

import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class AuthInterceptorService implements HttpInterceptor {

  constructor(private router: Router,
              private authStorageService: AuthStorageService,
              public toastr: ToastsManager) {
  }

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request)
      .catch((resp) => {
        if (resp instanceof HttpErrorResponse) {
          switch (resp.status) {
            case 404:
              this.router.navigate(['/404']);
              break;
            case 401:
              this.authStorageService.remove();
              this.router.navigate(['/login']);
              // falls through
            default:
              this.toastr.error(resp.error.detail || '',
                `${resp.status} - ${resp.statusText}`);
          }
        }
        // Return the error to the method that called it.
        return Observable.throw(resp);
      });
  }
}
