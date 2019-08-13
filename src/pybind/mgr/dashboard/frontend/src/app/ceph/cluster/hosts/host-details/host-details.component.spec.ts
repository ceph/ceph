import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { TabsetComponent, TabsModule } from 'ngx-bootstrap/tabs';

import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permissions } from '../../../../shared/models/permissions';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryComponent } from '../../inventory/inventory.component';
import { ServicesComponent } from '../../services/services.component';
import { HostDetailsComponent } from './host-details.component';

describe('HostDetailsComponent', () => {
  let component: HostDetailsComponent;
  let fixture: ComponentFixture<HostDetailsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      BsDropdownModule.forRoot(),
      RouterTestingModule,
      SharedModule
    ],
    declarations: [HostDetailsComponent, InventoryComponent, ServicesComponent],
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
    spyOn(orchService, 'inventoryList').and.returnValue(of([]));
    spyOn(orchService, 'serviceList').and.returnValue(of([]));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Host details tabset', () => {
    beforeEach(() => {
      component.selection.selected = [
        {
          hostname: 'localhost'
        }
      ];
      component.selection.update();
    });

    it('should recognize a tabset child', () => {
      fixture.detectChanges();
      const tabsetChild: TabsetComponent = component.tabsetChild;
      expect(tabsetChild).toBeDefined();
    });

    it('should show tabs', () => {
      fixture.detectChanges();
      const tabs = component.tabsetChild.tabs;
      expect(tabs.length).toBe(3);
      expect(tabs[0].heading).toBe('Inventory');
      expect(tabs[1].heading).toBe('Services');
      expect(tabs[2].heading).toBe('Performance Details');
    });
  });
});
