import { HTTP_INTERCEPTORS, HttpClient, HttpErrorResponse } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AppModule } from '../../app.module';
import { ApiInterceptorService } from './api-interceptor.service';
import { NotificationService } from './notification.service';

describe('ApiInterceptorService', () => {
  let notificationService: NotificationService;
  let httpTesting: HttpTestingController;
  let httpClient: HttpClient;
  let router: Router;
  const url = 'api/xyz';

  const runRouterTest = (errorOpts, expectedCallParams, done) => {
    httpClient.get(url).subscribe(
      () => {},
      (resp) => {
        // Error must have been forwarded by the interceptor.
        expect(resp instanceof HttpErrorResponse).toBeTruthy();
        done();
      }
    );
    httpTesting.expectOne(url).error(new ErrorEvent('abc'), errorOpts);
    httpTesting.verify();
    expect(router.navigate).toHaveBeenCalledWith(...expectedCallParams);
  };

  const runNotificationTest = (error, errorOpts, expectedCallParams, done) => {
    httpClient.get(url).subscribe(
      () => {},
      (resp) => {
        // Error must have been forwarded by the interceptor.
        expect(resp instanceof HttpErrorResponse).toBeTruthy();
        done();
      }
    );
    httpTesting.expectOne(url).error(error, errorOpts);
    httpTesting.verify();
    expect(notificationService.show).toHaveBeenCalledWith(...expectedCallParams);
  };

  configureTestBed({
    imports: [AppModule, HttpClientTestingModule],
    providers: [NotificationService]
  });

  beforeEach(() => {
    httpClient = TestBed.get(HttpClient);
    httpTesting = TestBed.get(HttpTestingController);
    notificationService = TestBed.get(NotificationService);
    spyOn(notificationService, 'show');
    router = TestBed.get(Router);
    spyOn(router, 'navigate');
  });

  it('should be created', () => {
    const service = TestBed.get(ApiInterceptorService);
    expect(service).toBeTruthy();
  });

  it('should redirect 401', (done) => {
    runRouterTest(
      {
        status: 401
      },
      [['/login']],
      done
    );
  });

  it('should redirect 403', (done) => {
    runRouterTest(
      {
        status: 403
      },
      [['/403']],
      done
    );
  });

  it('should redirect 404', (done) => {
    runRouterTest(
      {
        status: 404
      },
      [['/404']],
      done
    );
  });

  it('should show notification (error string)', function(done) {
    runNotificationTest(
      'foobar',
      {
        status: 500,
        statusText: 'Foo Bar'
      },
      [0, '500 - Foo Bar', 'foobar'],
      done
    );
  });

  it('should show notification (error object, triggered from backend)', function(done) {
    runNotificationTest(
      { detail: 'abc' },
      {
        status: 504,
        statusText: 'AAA bbb CCC'
      },
      [0, '504 - AAA bbb CCC', 'abc'],
      done
    );
  });

  it('should show notification (error object with unknown keys)', function(done) {
    runNotificationTest(
      { type: 'error' },
      {
        status: 0,
        statusText: 'Unknown Error',
        message: 'Http failure response for (unknown url): 0 Unknown Error',
        name: 'HttpErrorResponse',
        ok: false,
        url: null
      },
      [0, '0 - Unknown Error', 'Http failure response for api/xyz: 0 Unknown Error'],
      done
    );
  });

  it('should show notification (undefined error)', function(done) {
    runNotificationTest(
      undefined,
      {
        status: 502
      },
      [0, '502 - Unknown Error', 'Http failure response for api/xyz: 502 '],
      done
    );
  });

  it('should show 400 notification', function(done) {
    spyOn(notificationService, 'notifyTask');
    httpClient.get(url).subscribe(
      () => {},
      (resp) => {
        // Error must have been forwarded by the interceptor.
        expect(resp instanceof HttpErrorResponse).toBeTruthy();
        done();
      }
    );
    httpTesting
      .expectOne(url)
      .error({ task: { name: 'mytask', metadata: { component: 'foobar' } } }, { status: 400 });
    httpTesting.verify();
    expect(notificationService.show).toHaveBeenCalledTimes(0);
    expect(notificationService.notifyTask).toHaveBeenCalledWith({
      exception: { task: { metadata: { component: 'foobar' }, name: 'mytask' } },
      metadata: { component: 'foobar' },
      name: 'mytask',
      success: false
    });
  });
});
