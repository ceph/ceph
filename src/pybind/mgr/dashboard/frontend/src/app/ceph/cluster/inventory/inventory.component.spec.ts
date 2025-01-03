import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { InventoryDevicesComponent } from './inventory-devices/inventory-devices.component';
import { InventoryComponent } from './inventory.component';

describe('InventoryComponent', () => {
  let component: InventoryComponent;
  let fixture: ComponentFixture<InventoryComponent>;
  let orchService: OrchestratorService;
  let hostService: HostService;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      FormsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    declarations: [InventoryComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InventoryComponent);
    component = fixture.componentInstance;
    orchService = TestBed.inject(OrchestratorService);
    hostService = TestBed.inject(HostService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    spyOn(hostService, 'inventoryDeviceList').and.callThrough();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should not display doc panel if orchestrator is available', () => {
    expect(component.showDocPanel).toBeFalsy();
  });

  describe('after ngOnInit', () => {
    it('should load devices', () => {
      fixture.detectChanges();
      component.refresh(); // click refresh button
      expect(hostService.inventoryDeviceList).toHaveBeenNthCalledWith(1, undefined, false);

      const newHost = 'host0';
      component.hostname = newHost;
      fixture.detectChanges();
      component.ngOnChanges();
      expect(hostService.inventoryDeviceList).toHaveBeenNthCalledWith(2, newHost, false);
      component.refresh(); // click refresh button
      expect(hostService.inventoryDeviceList).toHaveBeenNthCalledWith(3, newHost, true);
    });
  });
});
