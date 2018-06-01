import { HttpClient } from '@angular/common/http';
import { Component } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../unit-test-helper';
import { ModuleStatusGuardService } from './module-status-guard.service';

describe('ModuleStatusGuardService', () => {
  let service: ModuleStatusGuardService;

  @Component({ selector: 'cd-foo', template: '' })
  class FooComponent {}

  const fakeService = {
    get: () => true
  };

  const routes: Routes = [{ path: '**', component: FooComponent }];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [ModuleStatusGuardService, { provide: HttpClient, useValue: fakeService }],
    declarations: [FooComponent]
  }, true);

  beforeEach(() => {
    service = TestBed.get(ModuleStatusGuardService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it(
    'should test canActivate with status available',
    fakeAsync(() => {
      let result = false;
      const route = new ActivatedRouteSnapshot();
      route.data = {
        moduleStatusGuardConfig: {
          apiPath: 'bar',
          redirectTo: 'foo'
        }
      };
      const httpClient = TestBed.get(HttpClient);
      spyOn(httpClient, 'get').and.returnValue(observableOf({ available: true, message: 'foo' }));
      service.canActivate(route, null).subscribe((resp) => {
        result = resp;
      });

      tick();
      expect(result).toBe(true);
    })
  );

  it(
    'should test canActivateChild with status unavailable',
    fakeAsync(() => {
      let result = true;
      const route = new ActivatedRouteSnapshot();
      route.data = {
        moduleStatusGuardConfig: {
          apiPath: 'bar',
          redirectTo: '/foo'
        }
      };
      const httpClient = TestBed.get(HttpClient);
      const router = TestBed.get(Router);
      spyOn(httpClient, 'get').and.returnValue(observableOf({ available: false, message: null }));
      service.canActivateChild(route, null).subscribe((resp) => {
        result = resp;
      });

      tick();
      expect(result).toBe(false);
      expect(router.url).toBe('/foo/');
    })
  );

  it(
    'should test canActivateChild with status unavailable',
    fakeAsync(() => {
      let result = true;
      const route = new ActivatedRouteSnapshot();
      route.data = {
        moduleStatusGuardConfig: {
          apiPath: 'bar',
          redirectTo: '/foo'
        }
      };
      const httpClient = TestBed.get(HttpClient);
      const router = TestBed.get(Router);
      spyOn(httpClient, 'get').and.returnValue(observableOf(null));
      service.canActivateChild(route, null).subscribe((resp) => {
        result = resp;
      });

      tick();
      expect(result).toBe(false);
      expect(router.url).toBe('/foo');
    })
  );
});
