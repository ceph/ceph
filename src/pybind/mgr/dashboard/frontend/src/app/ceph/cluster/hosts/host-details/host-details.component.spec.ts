import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { Permissions } from '../../../../shared/models/permissions';
import { SharedModule } from '../../../../shared/shared.module';
import { CephModule } from '../../../ceph.module';
import { CephSharedModule } from '../../../shared/ceph-shared.module';
import { HostDetailsComponent } from './host-details.component';

describe('HostDetailsComponent', () => {
  let component: HostDetailsComponent;
  let fixture: ComponentFixture<HostDetailsComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      TabsModule.forRoot(),
      BsDropdownModule.forRoot(),
      NgBootstrapFormValidationModule.forRoot(),
      RouterTestingModule,
      CephModule,
      CoreModule,
      CephSharedModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    declarations: [],
    providers: [i18nProviders]
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
      const tabsetChild = component.tabsetChild;
      expect(tabsetChild).toBeDefined();
    });

    it('should show tabs', () => {
      expect(component.tabsetChild.tabs.map((t) => t.heading)).toEqual([
        'Devices',
        'Device health',
        'Inventory',
        'Daemons',
        'Performance Details'
      ]);
    });
  });
});
