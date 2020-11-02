import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { InventoryDevicesComponent } from './inventory-devices/inventory-devices.component';
import { InventoryComponent } from './inventory.component';

describe('InventoryComponent', () => {
  let component: InventoryComponent;
  let fixture: ComponentFixture<InventoryComponent>;
  let orchService: OrchestratorService;

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
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    spyOn(orchService, 'inventoryDeviceList').and.callThrough();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    it('should load devices', () => {
      fixture.detectChanges();
      expect(orchService.inventoryDeviceList).toHaveBeenNthCalledWith(1, undefined, false);
      component.refresh(); // click refresh button
      expect(orchService.inventoryDeviceList).toHaveBeenNthCalledWith(2, undefined, true);

      const newHost = 'host0';
      component.hostname = newHost;
      fixture.detectChanges();
      component.ngOnChanges();
      expect(orchService.inventoryDeviceList).toHaveBeenNthCalledWith(3, newHost, false);
      component.refresh(); // click refresh button
      expect(orchService.inventoryDeviceList).toHaveBeenNthCalledWith(4, newHost, true);
    });
  });
});
