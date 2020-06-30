import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '../../inventory/inventory-devices/inventory-devices.component';
import { OsdDevicesSelectionModalComponent } from './osd-devices-selection-modal.component';

describe('OsdDevicesSelectionModalComponent', () => {
  let component: OsdDevicesSelectionModalComponent;
  let fixture: ComponentFixture<OsdDevicesSelectionModalComponent>;
  const devices: InventoryDevice[] = [
    {
      hostname: 'node0',
      uid: '1',
      path: 'sda',
      sys_api: {
        vendor: 'AAA',
        model: 'aaa',
        size: 1024,
        rotational: 'false',
        human_readable_size: '1 KB'
      },
      available: false,
      rejected_reasons: [''],
      device_id: 'AAA-aaa-id0',
      human_readable_type: 'nvme/ssd',
      osd_ids: []
    }
  ];

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
    providers: [NgbActiveModal, i18nProviders],
    declarations: [OsdDevicesSelectionModalComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDevicesSelectionModalComponent);
    component = fixture.componentInstance;
    component.devices = devices;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should disable submit button initially', () => {
    expectSubmitButton(false);
  });

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
