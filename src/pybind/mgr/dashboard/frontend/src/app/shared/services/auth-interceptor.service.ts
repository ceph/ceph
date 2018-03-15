import {
  HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest,
  HttpResponse
} from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';
import 'rxjs/add/operator/do';
import { Observable } from 'rxjs/Observable';

import { AuthStorageService } from './auth-storage.service';

@Injectable()
export class AuthInterceptorService implements HttpInterceptor {

  constructor(private router: Router,
              private authStorageService: AuthStorageService,
              public toastr: ToastsManager) {
  }

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).do((event: HttpEvent<any>) => {
      if (event instanceof HttpResponse) {
        // do nothing
      }
    }, (err: any) => {
      if (err instanceof HttpErrorResponse) {
        if (err.status === 404) {
          this.router.navigate(['/404']);
          return;
        }

        this.toastr.error(err.error.detail || '', `${err.status} - ${err.statusText}`);
        if (err.status === 401) {
          this.authStorageService.remove();
          this.router.navigate(['/login']);
        }
      }
    });
  }
}
