import { HttpClient } from '@angular/common/http';
import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf, Subject, throwError } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrModuleService } from '../api/mgr-module.service';
import { ModuleStatusGuardService } from './module-status-guard.service';
import { AuthStorageService } from './auth-storage.service';

import { CdDatePipe } from '../pipes/cd-date.pipe';
import { SharedModule } from '../shared.module';

describe('ModuleStatusGuardService', () => {
  let service: ModuleStatusGuardService;
  let httpClient: HttpClient;
  let router: Router;
  let route: ActivatedRouteSnapshot;
  let ngZone: NgZone;
  let mgrModuleService: MgrModuleService;
  let authStorageService: AuthStorageService;

  @Component({ selector: 'cd-foo', template: '', standalone: false })
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
    spyOn(authStorageService, 'getPermissions').and.returnValue({
      configOpt: { read: configOptPermission }
    } as any);
    const orchBackend = { orchestrator: backend };
    const getConfigSpy = spyOn(mgrModuleService, 'getConfig');
    getConfigSpy.and.returnValue(configOptPermission ? observableOf(orchBackend) : throwError({}));
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
    imports: [RouterTestingModule.withRoutes(routes), SharedModule],
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
    authStorageService = TestBed.inject(AuthStorageService);
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

  it('should skip backend check for user without config-opt permission', fakeAsync(() => {
    testCanActivate({ available: true, message: 'foo' }, true, '/', 'rook', false);
  }));

  it('should allow nested child routes without moduleStatusGuardConfig', fakeAsync(() => {
    const child = new ActivatedRouteSnapshot();
    child.url = [];
    child.data = { breadcrumbs: 'Overview', section: 'overview' };
    // Simulate a root snapshot (i.e. one with no parent) the way the real
    // router would, since a standalone snapshot has no `_routerState` set.
    (child as any)._routerState = { parent: () => null };

    spyOn(httpClient, 'get').and.returnValue(observableOf({ available: true, message: 'foo' }));
    let result: boolean;
    ngZone.run(() => {
      service.canActivateChild(child).subscribe((resp) => {
        result = resp;
      });
    });
    tick();
    expect(result).toBe(true);
    expect(httpClient.get).not.toHaveBeenCalled();
  }));

  describe('asynchronous backend fallback', () => {
    let status: Subject<{ available: boolean; message: string }>;
    let backend: Subject<object>;
    let results: boolean[];

    beforeEach(() => {
      status = new Subject();
      backend = new Subject();
      results = [];
      spyOn(httpClient, 'get').and.returnValue(status.asObservable());
      spyOn(mgrModuleService, 'getConfig').and.returnValue(backend.asObservable());
      spyOn(authStorageService, 'getPermissions').and.returnValue({
        configOpt: { read: true }
      } as any);
      spyOn(router, 'navigate').and.returnValue(Promise.resolve(true));
    });

    afterEach(() => {
      status.complete();
      backend.complete();
    });

    it('waits for the selected backend before deciding an unavailable status', () => {
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.next({ available: false, message: 'initializing' });
      status.complete();
      expect(results).toEqual([]);

      backend.next({ orchestrator: 'rook' });
      backend.complete();
      expect(results).toEqual([true]);
      expect(router.navigate).not.toHaveBeenCalled();
    });

    it('does not query the backend when the module is available', () => {
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.next({ available: true, message: null });

      expect(results).toEqual([true]);
      expect(mgrModuleService.getConfig).not.toHaveBeenCalled();
      expect(router.navigate).not.toHaveBeenCalled();
    });

    it('waits for the backend using the parent route configuration', () => {
      const child = new ActivatedRouteSnapshot();
      child.url = [];
      child.data = { breadcrumbs: 'Overview' };
      jest.spyOn(child, 'parent', 'get').mockReturnValue(route);

      service.canActivateChild(child).subscribe((allowed) => results.push(allowed));
      status.next({ available: false, message: 'initializing' });
      status.complete();
      expect(results).toEqual([]);
      expect(httpClient.get).toHaveBeenCalledWith('ui-api/bar/status');

      backend.next({ orchestrator: 'rook' });
      backend.complete();
      expect(results).toEqual([true]);
      expect(router.navigate).not.toHaveBeenCalled();
    });

    it('denies access when an unavailable module has a failed backend lookup', () => {
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.next({ available: false, message: 'initializing' });
      backend.error(new Error('backend lookup failed'));

      expect(results).toEqual([false]);
      expect(router.navigate).toHaveBeenCalledWith(
        ['/foo'],
        jasmine.objectContaining({ state: jasmine.objectContaining({ message: 'initializing' }) })
      );
    });

    it('denies access on a failed status lookup without querying the backend', () => {
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.error(new Error('status lookup failed'));

      expect(results).toEqual([false]);
      expect(mgrModuleService.getConfig).not.toHaveBeenCalled();
      expect(router.navigate).toHaveBeenCalledWith(['/foo']);
    });

    it('skips the fallback when the user cannot read the configuration', () => {
      (authStorageService.getPermissions as jasmine.Spy).and.returnValue({ configOpt: {} });
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.next({ available: false, message: 'initializing' });

      expect(results).toEqual([false]);
      expect(mgrModuleService.getConfig).not.toHaveBeenCalled();
    });

    it('skips the fallback when no backend is required', () => {
      delete route.data.moduleStatusGuardConfig.backend;
      service.canActivate(route).subscribe((allowed) => results.push(allowed));
      status.next({ available: false, message: 'initializing' });

      expect(results).toEqual([false]);
      expect(mgrModuleService.getConfig).not.toHaveBeenCalled();
    });

    it('cancels a pending backend lookup when navigation is canceled', () => {
      const subscription = service.canActivate(route).subscribe();
      status.next({ available: false, message: 'initializing' });
      status.complete();
      expect(backend.observers.length).toBe(1);

      subscription.unsubscribe();
      expect(backend.observers.length).toBe(0);
    });
  });
});
