import { HttpClient } from '@angular/common/http';
import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ModuleStatusGuardService } from './module-status-guard.service';

describe('ModuleStatusGuardService', () => {
  let service: ModuleStatusGuardService;
  let httpClient: HttpClient;
  let router: Router;
  let route: ActivatedRouteSnapshot;
  let ngZone: NgZone;

  @Component({ selector: 'cd-foo', template: '' })
  class FooComponent {}

  const fakeService = {
    get: () => true
  };

  const routes: Routes = [{ path: '**', component: FooComponent }];

  const testCanActivate = (getResult: {}, activateResult: boolean, urlResult: string) => {
    let result: boolean;
    spyOn(httpClient, 'get').and.returnValue(observableOf(getResult));
    ngZone.run(() => {
      service.canActivateChild(route).subscribe((resp) => {
        result = resp;
      });
    });

    tick();
    expect(result).toBe(activateResult);
    expect(router.url).toBe(urlResult);
  };

  configureTestBed(
    {
      imports: [RouterTestingModule.withRoutes(routes)],
      providers: [ModuleStatusGuardService, { provide: HttpClient, useValue: fakeService }],
      declarations: [FooComponent]
    },
    true
  );

  beforeEach(() => {
    service = TestBed.get(ModuleStatusGuardService);
    httpClient = TestBed.get(HttpClient);
    router = TestBed.get(Router);
    route = new ActivatedRouteSnapshot();
    route.data = {
      moduleStatusGuardConfig: {
        apiPath: 'bar',
        redirectTo: '/foo'
      }
    };
    ngZone = TestBed.get(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should test canActivate with status available', fakeAsync(() => {
    route.data.moduleStatusGuardConfig.redirectTo = 'foo';
    testCanActivate({ available: true, message: 'foo' }, true, '/');
  }));

  it('should test canActivateChild with status unavailable', fakeAsync(() => {
    testCanActivate({ available: false, message: null }, false, '/foo/');
  }));

  it('should test canActivateChild with status unavailable', fakeAsync(() => {
    testCanActivate(null, false, '/foo');
  }));
});
