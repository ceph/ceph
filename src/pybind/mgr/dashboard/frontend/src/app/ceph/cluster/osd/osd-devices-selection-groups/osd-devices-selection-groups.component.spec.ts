import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-devices.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FixtureHelper, Mocks } from '~/testing/unit-test-helper';
import { OsdDevicesSelectionGroupsComponent } from './osd-devices-selection-groups.component';

describe('OsdDevicesSelectionGroupsComponent', () => {
  let component: OsdDevicesSelectionGroupsComponent;
  let fixture: ComponentFixture<OsdDevicesSelectionGroupsComponent>;
  let fixtureHelper: FixtureHelper;
  const devices: InventoryDevice[] = [Mocks.getInventoryDevice('node0', '1')];

  const clearTextSelector = '.tc_clearSelections';
  const getClearText = () => {
    const debugElement = fixtureHelper.getElementByCss(clearTextSelector);
    return debugElement.nativeElement;
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      FormsModule,
      HttpClientTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [OsdDevicesSelectionGroupsComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDevicesSelectionGroupsComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    component.type = 'data';
    component.canSelect = true;
    component.inlineSelection = true;
  });

  describe('without available devices', () => {
    beforeEach(() => {
      component.availDevices = [];
      fixture.detectChanges();
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should not display filter form', () => {
      fixtureHelper.expectElementVisible('.device-filter-form', false);
    });

    it('should display devices table', () => {
      fixtureHelper.expectElementVisible('cd-inventory-devices', false);
    });
  });

  describe('without devices selected', () => {
    beforeEach(() => {
      component.availDevices = devices;
      fixture.detectChanges();
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should display filter form', () => {
      fixtureHelper.expectElementVisible('.device-filter-form', true);
    });

    it('should display empty devices table', () => {
      fixtureHelper.expectElementVisible('cd-inventory-devices', true);
      expect(component.tableDevices).toEqual([]);
    });
  });

  describe('with devices selected', () => {
    beforeEach(() => {
      component.isOsdPage = true;
      component.availDevices = devices;
      component.devices = devices;
      component.appliedFilters = [
        {
          name: 'Type',
          prop: 'human_readable_type',
          value: { raw: 'nvme/ssd', formatted: 'nvme/ssd' }
        }
      ];
      component.selectedFilters = { human_readable_type: 'nvme/ssd' };
      component.ngOnInit();
      fixture.detectChanges();
    });

    it('should display clear link', () => {
      const text = getClearText();
      expect(text).toBeTruthy();
      expect(text.textContent).toBe('Clear');
    });

    it('should display devices table', () => {
      fixtureHelper.expectElementVisible('cd-inventory-devices', true);
    });

    it('should clear devices by clicking Clear link', () => {
      spyOn(component.cleared, 'emit');
      fixtureHelper.clickElement(clearTextSelector);
      fixtureHelper.expectElementVisible('cd-inventory-devices', true);
      const event: Record<string, any> = {
        type: 'data',
        clearedDevices: devices
      };
      expect(component.cleared.emit).toHaveBeenCalledWith(event);
      expect(component.devices).toEqual([]);
    });
  });

  describe('wal inline selection', () => {
    beforeEach(() => {
      component.type = 'wal';
      component.name = 'WAL';
      component.availDevices = devices;
      component.canSelect = true;
      component.inlineSelection = true;
      fixture.detectChanges();
    });

    it('should display filter form instead of Add button', () => {
      fixtureHelper.expectElementVisible('.device-filter-form', true);
      fixtureHelper.expectElementVisible('cd-inventory-devices', true);
      fixtureHelper.expectElementVisible('.cd-col-form-input button.btn-light', false);
    });

    it('should display empty devices table before filters are applied', () => {
      expect(component.tableDevices).toEqual([]);
    });

    it('should auto-apply filtered devices when a required filter is selected', () => {
      spyOn(component.selected, 'emit');
      component.onFilterFieldChange('human_readable_type', 'nvme/ssd');
      fixture.detectChanges();

      expect(component.tableDevices.length).toBe(1);
      expect(component.devices.length).toBe(1);
      expect(component.selected.emit).toHaveBeenCalled();
    });
  });

  describe('wal inline selection without primary devices', () => {
    beforeEach(() => {
      component.type = 'wal';
      component.name = 'WAL';
      component.availDevices = devices;
      component.canSelect = false;
      component.inlineSelection = true;
      fixture.detectChanges();
    });

    it('should display add primary first alert without filter form', () => {
      fixtureHelper.expectElementVisible('.device-filter-form', false);
      fixtureHelper.expectElementVisible('cd-inventory-devices', false);
    });
  });

  describe('db inline selection', () => {
    beforeEach(() => {
      component.type = 'db';
      component.name = 'DB';
      component.availDevices = devices;
      component.canSelect = true;
      component.inlineSelection = true;
      fixture.detectChanges();
    });

    it('should display filter form instead of Add button', () => {
      fixtureHelper.expectElementVisible('.device-filter-form', true);
      fixtureHelper.expectElementVisible('cd-inventory-devices', true);
      fixtureHelper.expectElementVisible('.cd-col-form-input button.btn-light', false);
    });

    it('should auto-apply filtered devices when a required filter is selected', () => {
      spyOn(component.selected, 'emit');
      component.onFilterFieldChange('sys_api.vendor', 'AAA');
      fixture.detectChanges();

      expect(component.tableDevices.length).toBe(1);
      expect(component.devices.length).toBe(1);
      expect(component.selected.emit).toHaveBeenCalled();
    });
  });
});
