import { APP_BASE_HREF } from '@angular/common';
import { HTTP_INTERCEPTORS, provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { ErrorHandler, NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { ToastrModule } from 'ngx-toastr';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { CephModule } from './ceph/ceph.module';
import { CoreModule } from './core/core.module';
import { ApiInterceptorService } from './shared/services/api-interceptor.service';
import { JsErrorHandler } from './shared/services/js-error-handler.service';
import { SharedModule } from './shared/shared.module';

// Configuration for toast notifications
export const TOAST_CONFIG = {
  textBranding: true, // Set to true to show "Ceph" text, false to use icon
  brandingEnabled: true // Set to false to disable branding completely
};

@NgModule({
  declarations: [AppComponent],
  exports: [SharedModule],
  bootstrap: [AppComponent],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    ToastrModule.forRoot({
      positionClass: 'toast-top-right',
      preventDuplicates: true,
      enableHtml: true,
      timeOut: 5000,
      closeButton: true,
      progressBar: true,
      easing: 'ease-in-out',
      easeTime: 300,
      tapToDismiss: true,
      maxOpened: 3,
      autoDismiss: true,
      newestOnTop: true,
      extendedTimeOut: 1000
    }),
    AppRoutingModule,
    CoreModule,
    SharedModule,
    CephModule
  ],
  providers: [
    {
      provide: ErrorHandler,
      useClass: JsErrorHandler
    },
    {
      provide: HTTP_INTERCEPTORS,
      useClass: ApiInterceptorService,
      multi: true
    },
    {
      provide: APP_BASE_HREF,
      useValue: '/' + (window.location.pathname.split('/', 1)[1] || '')
    },
    provideHttpClient(withInterceptorsFromDi())
  ]
})
export class AppModule {}
