import { registerLocaleData } from '@angular/common';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { ErrorHandler, NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { JwtModule } from '@auth0/angular-jwt';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { BlockUIModule } from 'ng-block-ui';
import { AccordionModule } from 'ngx-bootstrap/accordion';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { CephModule } from './ceph/ceph.module';
import { CoreModule } from './core/core.module';
import { i18nProviders, LocaleHelper } from './locale.helper';
import { ApiInterceptorService } from './shared/services/api-interceptor.service';
import { JsErrorHandler } from './shared/services/js-error-handler.service';
import { SharedModule } from './shared/shared.module';

export function jwtTokenGetter() {
  return localStorage.getItem('access_token');
}

registerLocaleData(LocaleHelper.getLocaleData(), LocaleHelper.getLocale());

@NgModule({
  declarations: [AppComponent],
  imports: [
    HttpClientModule,
    BlockUIModule.forRoot(),
    BrowserModule,
    BrowserAnimationsModule,
    ToastrModule.forRoot({
      positionClass: 'toast-top-right',
      preventDuplicates: true,
      enableHtml: true
    }),
    AppRoutingModule,
    CoreModule,
    SharedModule,
    CephModule,
    AccordionModule.forRoot(),
    BsDropdownModule.forRoot(),
    TabsModule.forRoot(),
    JwtModule.forRoot({
      config: {
        tokenGetter: jwtTokenGetter
      }
    })
  ],
  exports: [SharedModule],
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
    i18nProviders,
    I18n
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
