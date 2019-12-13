import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
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
    component.selection = new CdTableSelection();
    component.permissions = new Permissions({
      hosts: ['read'],
      grafana: ['read']
    });
    const orchService = TestBed.get(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    spyOn(orchService, 'inventoryDeviceList').and.returnValue(of([]));
    spyOn(orchService, 'serviceList').and.returnValue(of([]));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Host details tabset', () => {
    beforeEach(() => {
      component.selection.selected = [{ hostname: 'localhost' }];
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
        'Services',
        'Performance Details'
      ]);
    });
  });
});
