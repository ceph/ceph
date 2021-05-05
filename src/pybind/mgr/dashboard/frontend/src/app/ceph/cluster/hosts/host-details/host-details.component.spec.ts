import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { CephModule } from '~/app/ceph/ceph.module';
import { CephSharedModule } from '~/app/ceph/shared/ceph-shared.module';
import { CoreModule } from '~/app/core/core.module';
import { Permissions } from '~/app/shared/models/permissions';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, TabHelper } from '~/testing/unit-test-helper';
import { HostDetailsComponent } from './host-details.component';

describe('HostDetailsComponent', () => {
  let component: HostDetailsComponent;
  let fixture: ComponentFixture<HostDetailsComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      CephModule,
      CoreModule,
      CephSharedModule,
      SharedModule,
      ToastrModule.forRoot()
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HostDetailsComponent);
    component = fixture.componentInstance;
    component.selection = undefined;
    component.permissions = new Permissions({
      hosts: ['read'],
      grafana: ['read']
    });
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Host details tabset', () => {
    beforeEach(() => {
      component.selection = { hostname: 'localhost' };
      fixture.detectChanges();
    });

    it('should recognize a tabset child', () => {
      const tabsetChild = TabHelper.getNgbNav(fixture);
      expect(tabsetChild).toBeDefined();
    });

    it('should show tabs', () => {
      expect(TabHelper.getTextContents(fixture)).toEqual([
        'Devices',
        'Physical Disks',
        'Daemons',
        'Performance Details',
        'Device health'
      ]);
    });
  });
});
