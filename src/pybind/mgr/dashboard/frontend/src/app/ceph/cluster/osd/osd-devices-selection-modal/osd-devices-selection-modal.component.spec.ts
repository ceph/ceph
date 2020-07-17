import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, Mocks } from '../../../../../testing/unit-test-helper';
import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '../../inventory/inventory-devices/inventory-devices.component';
import { OsdDevicesSelectionModalComponent } from './osd-devices-selection-modal.component';

describe('OsdDevicesSelectionModalComponent', () => {
  let component: OsdDevicesSelectionModalComponent;
  let fixture: ComponentFixture<OsdDevicesSelectionModalComponent>;
  let timeoutFn: Function;

  const devices: InventoryDevice[] = [Mocks.getInventoryDevice('node0', '1')];

  const expectSubmitButton = (enabled: boolean) => {
    const nativeElement = fixture.debugElement.nativeElement;
    const button = nativeElement.querySelector('.modal-footer button');
    expect(button.disabled).toBe(!enabled);
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      FormsModule,
      HttpClientTestingModule,
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [OsdDevicesSelectionModalComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    spyOn(window, 'setTimeout').and.callFake((fn) => (timeoutFn = fn));

    fixture = TestBed.createComponent(OsdDevicesSelectionModalComponent);
    component = fixture.componentInstance;
    component.devices = devices;

    // Mocks InventoryDeviceComponent
    component.inventoryDevices = {
      columns: [
        { name: 'Device path', prop: 'path' },
        {
          name: 'Type',
          prop: 'human_readable_type'
        },
        {
          name: 'Available',
          prop: 'available'
        }
      ]
    } as InventoryDevicesComponent;
    // Mocks the update from the above component
    component.filterColumns = ['path', 'human_readable_type'];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should disable submit button initially', () => {
    expectSubmitButton(false);
  });

  it(
    'should update requiredFilters after ngAfterViewInit is called to prevent ' +
      'ExpressionChangedAfterItHasBeenCheckedError',
    () => {
      expect(component.requiredFilters).toEqual([]);
      timeoutFn();
      expect(component.requiredFilters).toEqual(['Device path', 'Type']);
    }
  );

  it('should enable submit button after filtering some devices', () => {
    const event: CdTableColumnFiltersChange = {
      filters: [
        {
          name: 'hostname',
          prop: 'hostname',
          value: { raw: 'node0', formatted: 'node0' }
        },
        {
          name: 'size',
          prop: 'size',
          value: { raw: '1024', formatted: '1KiB' }
        }
      ],
      data: devices,
      dataOut: []
    };
    component.onFilterChange(event);
    fixture.detectChanges();
    expectSubmitButton(true);
  });
});
