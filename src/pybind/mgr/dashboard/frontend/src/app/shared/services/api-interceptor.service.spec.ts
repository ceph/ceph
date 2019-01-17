import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { AppModule } from '../../app.module';
import { CdNotification, CdNotificationConfig } from '../models/cd-notification';
import { ApiInterceptorService } from './api-interceptor.service';
import { NotificationService } from './notification.service';

describe('ApiInterceptorService', () => {
  let notificationService: NotificationService;
  let httpTesting: HttpTestingController;
  let httpClient: HttpClient;
  let router: Router;
  const url = 'api/xyz';

  const httpError = (error, errorOpts, done = (resp) => {}) => {
    httpClient.get(url).subscribe(
      () => {},
      (resp) => {
        // Error must have been forwarded by the interceptor.
        expect(resp instanceof HttpErrorResponse).toBeTruthy();
        done(resp);
      }
    );
    httpTesting.expectOne(url).error(error, errorOpts);
  };

  const runRouterTest = (errorOpts, expectedCallParams) => {
    httpError(new ErrorEvent('abc'), errorOpts);
    httpTesting.verify();
    expect(router.navigate).toHaveBeenCalledWith(...expectedCallParams);
  };

  const runNotificationTest = (error, errorOpts, expectedCallParams) => {
    httpError(error, errorOpts);
    httpTesting.verify();
    expect(notificationService.show).toHaveBeenCalled();
    expect(notificationService.save).toHaveBeenCalledWith(expectedCallParams);
  };

  const createCdNotification = (type, title?, message?, options?, application?) => {
    return new CdNotification(new CdNotificationConfig(type, title, message, options, application));
  };

  configureTestBed({
    imports: [AppModule, HttpClientTestingModule],
    providers: [
      NotificationService,
      i18nProviders,
      {
        provide: ToastsManager,
        useValue: {
          error: () => true
        }
      }
    ]
  });

  beforeEach(() => {
    const baseTime = new Date('2022-02-22');
    spyOn(global, 'Date').and.returnValue(baseTime);

    httpClient = TestBed.get(HttpClient);
    httpTesting = TestBed.get(HttpTestingController);

    notificationService = TestBed.get(NotificationService);
    spyOn(notificationService, 'show').and.callThrough();
    spyOn(notificationService, 'save');

    router = TestBed.get(Router);
    spyOn(router, 'navigate');
  });

  it('should be created', () => {
    const service = TestBed.get(ApiInterceptorService);
    expect(service).toBeTruthy();
  });

  describe('test different error behaviours', () => {
    beforeEach(() => {
      spyOn(window, 'setTimeout').and.callFake((fn) => fn());
    });

    it('should redirect 401', () => {
      runRouterTest(
        {
          status: 401
        },
        [['/login']]
      );
    });

    it('should redirect 403', () => {
      runRouterTest(
        {
          status: 403
        },
        [['/403']]
      );
    });

    it('should show notification (error string)', () => {
      runNotificationTest(
        'foobar',
        {
          status: 500,
          statusText: 'Foo Bar'
        },
        createCdNotification(0, '500 - Foo Bar', 'foobar')
      );
    });

    it('should show notification (error object, triggered from backend)', () => {
      runNotificationTest(
        { detail: 'abc' },
        {
          status: 504,
          statusText: 'AAA bbb CCC'
        },
        createCdNotification(0, '504 - AAA bbb CCC', 'abc')
      );
    });

    it('should show notification (error object with unknown keys)', () => {
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
        createCdNotification(
          0,
          '0 - Unknown Error',
          'Http failure response for api/xyz: 0 Unknown Error'
        )
      );
    });

    it('should show notification (undefined error)', () => {
      runNotificationTest(
        undefined,
        {
          status: 502
        },
        createCdNotification(0, '502 - Unknown Error', 'Http failure response for api/xyz: 502 ')
      );
    });

    it('should show 400 notification', () => {
      spyOn(notificationService, 'notifyTask');
      httpError({ task: { name: 'mytask', metadata: { component: 'foobar' } } }, { status: 400 });
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

  describe('interceptor error handling', () => {
    it('should show default behaviour', fakeAsync(() => {
      httpError(undefined, { status: 500 });
      tick(10);
      expect(notificationService.save).toHaveBeenCalled();
    }));

    it('should prevent the default behaviour with preventDefault', fakeAsync(() => {
      httpError(undefined, { status: 500 }, (resp) => resp.preventDefault());
      tick(10);
      expect(notificationService.save).not.toHaveBeenCalled();
    }));

    it('should prevent the default behaviour by status code', fakeAsync(() => {
      httpError(undefined, { status: 500 }, (resp) => resp.ignoreStatusCode(500));
      tick(10);
      expect(notificationService.save).not.toHaveBeenCalled();
    }));

    it('should use different application icon (default Ceph) in error message', fakeAsync(() => {
      const msg = 'Cannot connect to Alertmanager';
      httpError(undefined, { status: 500 }, (resp) => {
        (resp.application = 'Prometheus'), (resp.message = msg);
      });
      tick(10);
      expect(notificationService.save).toHaveBeenCalledWith(
        createCdNotification(0, '500 - Unknown Error', msg, undefined, 'Prometheus')
      );
    }));
  });
});
