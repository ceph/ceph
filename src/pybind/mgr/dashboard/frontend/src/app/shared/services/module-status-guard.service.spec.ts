import { HttpClient } from '@angular/common/http';
import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf, throwError } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrModuleService } from '../api/mgr-module.service';
import { ModuleStatusGuardService } from './module-status-guard.service';
import { ToastrModule } from 'ngx-toastr';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { SharedModule } from '../shared.module';

describe('ModuleStatusGuardService', () => {
  let service: ModuleStatusGuardService;
  let httpClient: HttpClient;
  let router: Router;
  let route: ActivatedRouteSnapshot;
  let ngZone: NgZone;
  let mgrModuleService: MgrModuleService;

  @Component({ selector: 'cd-foo', template: '' })
  class FooComponent {}

  const fakeService = {
    get: () => true
  };

  const routes: Routes = [{ path: '**', component: FooComponent }];

  const testCanActivate = (
    getResult: {},
    activateResult: boolean,
    urlResult: string,
    backend = 'cephadm',
    configOptPermission = true
  ) => {
    let result: boolean;
    spyOn(httpClient, 'get').and.returnValue(observableOf(getResult));
    const orchBackend = { orchestrator: backend };
    const getConfigSpy = spyOn(mgrModuleService, 'getConfig');
    configOptPermission
      ? getConfigSpy.and.returnValue(observableOf(orchBackend))
      : getConfigSpy.and.returnValue(throwError({}));
    ngZone.run(() => {
      service.canActivateChild(route).subscribe((resp) => {
        result = resp;
      });
    });

    tick();
    expect(result).toBe(activateResult);
    expect(router.url).toBe(urlResult);
  };

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes), ToastrModule.forRoot(), SharedModule],
    providers: [
      ModuleStatusGuardService,
      { provide: HttpClient, useValue: fakeService },
      CdDatePipe
    ],
    declarations: [FooComponent]
  });

  beforeEach(() => {
    service = TestBed.inject(ModuleStatusGuardService);
    httpClient = TestBed.inject(HttpClient);
    mgrModuleService = TestBed.inject(MgrModuleService);
    router = TestBed.inject(Router);
    route = new ActivatedRouteSnapshot();
    route.url = [];
    route.data = {
      moduleStatusGuardConfig: {
        uiApiPath: 'bar',
        redirectTo: '/foo',
        backend: 'rook'
      }
    };
    ngZone = TestBed.inject(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should test canActivate with status available', fakeAsync(() => {
    route.data.moduleStatusGuardConfig.redirectTo = 'foo';
    testCanActivate({ available: true, message: 'foo' }, true, '/');
  }));

  it('should test canActivateChild with status unavailable', fakeAsync(() => {
    testCanActivate({ available: false, message: null }, false, '/foo');
  }));

  it('should test canActivateChild with status unavailable', fakeAsync(() => {
    testCanActivate(null, false, '/foo');
  }));

  it('should redirect normally if the backend provided matches the current backend', fakeAsync(() => {
    testCanActivate({ available: true, message: 'foo' }, true, '/', 'rook');
  }));

  it('should redirect to the "redirectTo" link for user without sufficient permission', fakeAsync(() => {
    testCanActivate({ available: true, message: 'foo' }, true, '/foo', 'rook', false);
  }));
});
