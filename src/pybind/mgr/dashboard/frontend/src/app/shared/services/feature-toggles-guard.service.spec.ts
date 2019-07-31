import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { FeatureTogglesGuardService } from './feature-toggles-guard.service';
import { FeatureTogglesService } from './feature-toggles.service';

describe('FeatureTogglesGuardService', () => {
  let service: FeatureTogglesGuardService;
  let fakeFeatureTogglesService: FeatureTogglesService;
  let router: Router;
  let ngZone: NgZone;

  @Component({ selector: 'cd-cephfs', template: '' })
  class CephfsComponent {}

  @Component({ selector: 'cd-404', template: '' })
  class NotFoundComponent {}

  const routes: Routes = [
    { path: 'cephfs', component: CephfsComponent },
    { path: '404', component: NotFoundComponent }
  ];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [
      { provide: FeatureTogglesService, useValue: { get: null } },
      FeatureTogglesGuardService
    ],
    declarations: [CephfsComponent, NotFoundComponent]
  });

  beforeEach(() => {
    service = TestBed.get(FeatureTogglesGuardService);
    fakeFeatureTogglesService = TestBed.get(FeatureTogglesService);
    ngZone = TestBed.get(NgZone);
    router = TestBed.get(Router);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  function testCanActivate(path, feature_toggles_map) {
    let result: boolean;
    spyOn(fakeFeatureTogglesService, 'get').and.returnValue(observableOf(feature_toggles_map));

    ngZone.run(() => {
      service
        .canActivate(<ActivatedRouteSnapshot>{ routeConfig: { path: path } })
        .subscribe((val) => (result = val));
    });
    tick();

    return result;
  }

  it('should allow the feature if enabled', fakeAsync(() => {
    expect(testCanActivate('cephfs', { cephfs: true })).toBe(true);
    expect(router.url).toBe('/');
  }));

  it('should redirect to 404 if disable', fakeAsync(() => {
    expect(testCanActivate('cephfs', { cephfs: false })).toBe(false);
    expect(router.url).toBe('/404');
  }));
});
