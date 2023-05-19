import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { PerformanceCounterModule } from '~/app/ceph/performance-counter/performance-counter.module';
import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, TabHelper } from '~/testing/unit-test-helper';
import { RgwDaemonDetailsComponent } from '../rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list.component';

describe('RgwDaemonListComponent', () => {
  let component: RgwDaemonListComponent;
  let fixture: ComponentFixture<RgwDaemonListComponent>;
  let getPermissionsSpy: jasmine.Spy;
  let getRealmsSpy: jasmine.Spy;
  let listDaemonsSpy: jest.SpyInstance;
  const permissions = new Permissions({ grafana: ['read'] });
  const daemon: RgwDaemon = {
    id: '8000',
    service_map_id: '4803',
    version: 'ceph version',
    server_hostname: 'ceph',
    realm_name: 'realm1',
    zonegroup_name: 'zg1-realm1',
    zone_name: 'zone1-zg1-realm1',
    default: true,
    port: 80
  };

  const expectTabsAndHeading = (length: number, heading: string) => {
    const tabs = TabHelper.getTextContents(fixture);
    expect(tabs.length).toEqual(length);
    expect(tabs[length - 1]).toEqual(heading);
  };

  configureTestBed({
    declarations: [RgwDaemonListComponent, RgwDaemonDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      NgbNavModule,
      PerformanceCounterModule,
      SharedModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    getPermissionsSpy = spyOn(TestBed.inject(AuthStorageService), 'getPermissions');
    getPermissionsSpy.and.returnValue(new Permissions({}));
    getRealmsSpy = spyOn(TestBed.inject(RgwSiteService), 'get');
    getRealmsSpy.and.returnValue(of([]));
    listDaemonsSpy = jest
      .spyOn(TestBed.inject(RgwDaemonService), 'list')
      .mockReturnValue(of([daemon]));
    fixture = TestBed.createComponent(RgwDaemonListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should show a row with daemon info', fakeAsync(() => {
    fixture.detectChanges();
    tick();
    expect(listDaemonsSpy).toHaveBeenCalledTimes(1);
    expect(component.daemons).toEqual([daemon]);
    expect(fixture.debugElement.query(By.css('cd-table')).nativeElement.textContent).toContain(
      'total of 1'
    );

    fixture.destroy();
  }));

  it('should only show Gateways List tab', () => {
    fixture.detectChanges();

    expectTabsAndHeading(1, 'Gateways List');
  });

  it('should show Overall Performance tab', () => {
    getPermissionsSpy.and.returnValue(permissions);
    fixture.detectChanges();

    expectTabsAndHeading(2, 'Overall Performance');
  });

  it('should show Sync Performance tab', () => {
    getPermissionsSpy.and.returnValue(permissions);
    getRealmsSpy.and.returnValue(of(['realm1']));
    fixture.detectChanges();

    expectTabsAndHeading(3, 'Sync Performance');
  });
});
