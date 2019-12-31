import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { getterForProp } from '@swimlane/ngx-datatable/release/utils';
import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';

import {
  configureTestBed,
  FixtureHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryDevice } from './inventory-device.model';
import { InventoryDevicesComponent } from './inventory-devices.component';

describe('InventoryDevicesComponent', () => {
  let component: InventoryDevicesComponent;
  let fixture: ComponentFixture<InventoryDevicesComponent>;
  let fixtureHelper: FixtureHelper;
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
    },
    {
      hostname: 'node0',
      uid: '2',
      path: 'sdb',
      sys_api: {
        vendor: 'AAA',
        model: 'aaa',
        size: 1024,
        rotational: 'false',
        human_readable_size: '1 KB'
      },
      available: true,
      rejected_reasons: [''],
      device_id: 'AAA-aaa-id1',
      human_readable_type: 'nvme/ssd',
      osd_ids: []
    },
    {
      hostname: 'node0',
      uid: '3',
      path: 'sdc',
      sys_api: {
        vendor: 'BBB',
        model: 'bbb',
        size: 2048,
        rotational: 'true',
        human_readable_size: '2 KB'
      },
      available: true,
      rejected_reasons: [''],
      device_id: 'BBB-bbbb-id0',
      human_readable_type: 'hdd',
      osd_ids: []
    },
    {
      hostname: 'node1',
      uid: '4',
      path: 'sda',
      sys_api: {
        vendor: 'CCC',
        model: 'ccc',
        size: 1024,
        rotational: 'true',
        human_readable_size: '1 KB'
      },
      available: false,
      rejected_reasons: [''],
      device_id: 'CCC-cccc-id0',
      human_readable_type: 'hdd',
      osd_ids: []
    }
  ];

  configureTestBed({
    imports: [FormsModule, HttpClientTestingModule, SharedModule, ToastrModule.forRoot()],
    providers: [i18nProviders],
    declarations: [InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InventoryDevicesComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('without device data', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should have columns that are sortable', () => {
      expect(component.columns.every((column) => Boolean(column.prop))).toBeTruthy();
    });

    it('should have filters', () => {
      const labelTexts = fixtureHelper.getTextAll('.tc_filter span:first-child');
      const filterLabels = _.map(component.filters, 'label');
      expect(labelTexts).toEqual(filterLabels);

      const optionTexts = fixtureHelper.getTextAll('.tc_filter option');
      expect(optionTexts).toEqual(_.map(component.filters, 'initValue'));
    });
  });

  describe('with device data', () => {
    beforeEach(() => {
      component.devices = devices;
      fixture.detectChanges();
    });

    it('should have filters', () => {
      for (let i = 0; i < component.filters.length; i++) {
        const optionTexts = fixtureHelper.getTextAll(`.tc_filter:nth-child(${i + 1}) option`);
        const optionTextsSet = new Set(optionTexts);

        const filter = component.filters[i];
        const columnValues = devices.map((device: InventoryDevice) => {
          const valueGetter = getterForProp(filter.prop);
          const value = valueGetter(device, filter.prop);
          const formatValue = filter.pipe ? filter.pipe.transform(value) : value;
          return `${formatValue}`;
        });
        const expectedOptionsSet = new Set(['*', ...columnValues]);
        expect(optionTextsSet).toEqual(expectedOptionsSet);
      }
    });

    it('should filter a single column', () => {
      spyOn(component.filterChange, 'emit');
      fixtureHelper.selectElement('.tc_filter:nth-child(1) select', 'node1');
      expect(component.filterInDevices.length).toBe(1);
      expect(component.filterInDevices[0]).toEqual(devices[3]);
      expect(component.filterChange.emit).toHaveBeenCalled();
    });

    it('should filter multiple columns', () => {
      spyOn(component.filterChange, 'emit');
      fixtureHelper.selectElement('.tc_filter:nth-child(2) select', 'hdd');
      fixtureHelper.selectElement('.tc_filter:nth-child(1) select', 'node0');
      expect(component.filterInDevices.length).toBe(1);
      expect(component.filterInDevices[0].uid).toBe('3');
      expect(component.filterChange.emit).toHaveBeenCalledTimes(2);
    });
  });
});
