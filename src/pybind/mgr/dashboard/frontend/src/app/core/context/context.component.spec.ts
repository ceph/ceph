import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { configureTestBed, RgwHelper } from '~/testing/unit-test-helper';
import { ContextComponent } from './context.component';

describe('ContextComponent', () => {
  let component: ContextComponent;
  let fixture: ComponentFixture<ContextComponent>;
  let router: Router;
  let routerNavigateByUrlSpy: jasmine.Spy;
  let routerNavigateSpy: jasmine.Spy;
  let getPermissionsSpy: jasmine.Spy;
  let getFeatureTogglesSpy: jasmine.Spy;
  let ftMap: FeatureTogglesMap;
  let httpTesting: HttpTestingController;

  const daemonList = RgwHelper.getDaemonList();

  configureTestBed({
    declarations: [ContextComponent],
    imports: [HttpClientTestingModule, RouterTestingModule]
  });

  beforeEach(() => {
    httpTesting = TestBed.inject(HttpTestingController);
    router = TestBed.inject(Router);
    routerNavigateByUrlSpy = spyOn(router, 'navigateByUrl');
    routerNavigateByUrlSpy.and.returnValue(Promise.resolve(undefined));
    routerNavigateSpy = spyOn(router, 'navigate');
    getPermissionsSpy = spyOn(TestBed.inject(AuthStorageService), 'getPermissions');
    getPermissionsSpy.and.returnValue(
      new Permissions({ rgw: ['read', 'update', 'create', 'delete'] })
    );
    getFeatureTogglesSpy = spyOn(TestBed.inject(FeatureTogglesService), 'get');
    ftMap = new FeatureTogglesMap();
    ftMap.rgw = true;
    getFeatureTogglesSpy.and.returnValue(of(ftMap));
    fixture = TestBed.createComponent(ContextComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should not show any info if not in RGW route', () => {
    component.isRgwRoute = false;
    expect(fixture.debugElement.nativeElement.textContent).toEqual('');
  });

  it('should select the default daemon', fakeAsync(() => {
    component.isRgwRoute = true;
    fixture.detectChanges();
    tick();
    const req = httpTesting.expectOne('api/rgw/daemon');
    req.flush(daemonList);
    fixture.detectChanges();
    const selectedDaemon = fixture.debugElement.nativeElement.querySelector(
      '.ctx-bar-selected-rgw-daemon'
    );
    expect(selectedDaemon.textContent).toEqual(' daemon2 ( zonegroup2 ) ');

    const availableDaemons = fixture.debugElement.nativeElement.querySelectorAll(
      '.ctx-bar-available-rgw-daemon'
    );
    expect(availableDaemons.length).toEqual(daemonList.length);
    expect(availableDaemons[0].textContent).toEqual(' daemon1 ( zonegroup1 ) ');
    component.ngOnDestroy();
  }));

  it('should select the chosen daemon', fakeAsync(() => {
    component.isRgwRoute = true;
    fixture.detectChanges();
    tick();
    const req = httpTesting.expectOne('api/rgw/daemon');
    req.flush(daemonList);
    fixture.detectChanges();
    component.onDaemonSelection(daemonList[2]);
    expect(routerNavigateByUrlSpy).toHaveBeenCalledTimes(1);
    fixture.detectChanges();
    tick();
    expect(routerNavigateSpy).toHaveBeenCalledTimes(1);
    const selectedDaemon = fixture.debugElement.nativeElement.querySelector(
      '.ctx-bar-selected-rgw-daemon'
    );
    expect(selectedDaemon.textContent).toEqual(' daemon3 ( zonegroup3 ) ');
    component.ngOnDestroy();
  }));
});
