import { ErrorHandler, Injectable, Injector } from '@angular/core';

import { LoggingService } from '../api/logging.service';

@Injectable()
export class JsErrorHandler implements ErrorHandler {
  constructor(private injector: Injector) {}

  handleError(error: any) {
    const loggingService = this.injector.get(LoggingService);
    const url = window.location.href;
    const message = error && error.message;
    const stack = error && error.stack;
    loggingService.jsError(url, message, stack).subscribe();
    throw error;
  }
}
