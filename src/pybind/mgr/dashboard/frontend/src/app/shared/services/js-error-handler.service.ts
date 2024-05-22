import { ErrorHandler, Injectable, Injector } from '@angular/core';
import { Router } from '@angular/router';

import { DashboardError } from '~/app/core/error/error';
import { LoggingService } from '../api/logging.service';

@Injectable()
export class JsErrorHandler implements ErrorHandler {
  constructor(private injector: Injector, private router: Router) {}

  handleError(error: any) {
    const loggingService = this.injector.get(LoggingService);
    const url = window.location.href;
    const message = error && error.message;
    const stack = error && error.stack;
    loggingService.jsError(url, message, stack).subscribe();
    if (error.rejection instanceof DashboardError) {
      setTimeout(
        () =>
          this.router.navigate(['error'], {
            state: {
              message: error.rejection.message,
              header: error.rejection.header,
              icon: error.rejection.icon
            }
          }),
        50
      );
    } else {
      throw error;
    }
  }
}
