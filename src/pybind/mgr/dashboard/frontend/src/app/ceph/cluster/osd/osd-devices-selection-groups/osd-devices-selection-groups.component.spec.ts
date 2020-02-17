import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { ToastrModule } from 'ngx-toastr';

import {
  configureTestBed,
  FixtureHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryDevicesComponent } from '../../inventory/inventory-devices/inventory-devices.component';
import { OsdDevicesSelectionGroupsComponent } from './osd-devices-selection-groups.component';

describe('OsdDevicesSelectionGroupsComponent', () => {
  let component: OsdDevicesSelectionGroupsComponent;
  let fixture: ComponentFixture<OsdDevicesSelectionGroupsComponent>;
  let fixtureHelper: FixtureHelper;
  const devices = [
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

  const buttonSelector = '.col-sm-9 button';
  const getButton = () => {
    const debugElement = fixtureHelper.getElementByCss(buttonSelector);
    return debugElement.nativeElement;
  };
  const clearTextSelector = '.tc_clearSelections';
  const getClearText = () => {
    const debugElement = fixtureHelper.getElementByCss(clearTextSelector);
    return debugElement.nativeElement;
  };

  configureTestBed({
    imports: [FormsModule, HttpClientTestingModule, SharedModule, ToastrModule.forRoot()],
    providers: [i18nProviders],
    declarations: [OsdDevicesSelectionGroupsComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDevicesSelectionGroupsComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    component.canSelect = true;
  });

  describe('without available devices', () => {
    beforeEach(() => {
      component.availDevices = [];
      fixture.detectChanges();
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should display Add button in disabled state', () => {
      const button = getButton();
      expect(button).toBeTruthy();
      expect(button.disabled).toBe(true);
      expect(button.textContent).toBe('Add');
    });

    it('should not display devices table', () => {
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

    it('should display Add button in enabled state', () => {
      const button = getButton();
      expect(button).toBeTruthy();
      expect(button.disabled).toBe(false);
      expect(button.textContent).toBe('Add');
    });

    it('should not display devices table', () => {
      fixtureHelper.expectElementVisible('cd-inventory-devices', false);
    });
  });

  describe('with devices selected', () => {
    beforeEach(() => {
      component.availDevices = [];
      component.devices = devices;
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
      fixtureHelper.expectElementVisible('cd-inventory-devices', false);
      const event = {
        type: undefined,
        clearedDevices: devices
      };
      expect(component.cleared.emit).toHaveBeenCalledWith(event);
    });
  });
});
