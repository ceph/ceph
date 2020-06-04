import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SimpleChange, SimpleChanges } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { OsdService } from '../../../shared/api/osd.service';
import { HddSmartDataV1, NvmeSmartDataV1, SmartDataResult } from '../../../shared/models/smart';
import { SharedModule } from '../../../shared/shared.module';
import { SmartListComponent } from './smart-list.component';

describe('OsdSmartListComponent', () => {
  let component: SmartListComponent;
  let fixture: ComponentFixture<SmartListComponent>;
  let osdService: OsdService;

  const SMART_DATA_HDD_VERSION_1_0: HddSmartDataV1 = require('./fixtures/smart_data_version_1_0_hdd_response.json');
  const SMART_DATA_NVME_VERSION_1_0: NvmeSmartDataV1 = require('./fixtures/smart_data_version_1_0_nvme_response.json');

  /**
   * Sets attributes for _all_ returned devices according to the given path. The syntax is the same
   * as used in lodash.set().
   *
   * @example
   * patchData('json_format_version', [2, 0])  // sets the value of `json_format_version` to [2, 0]
   *                                           // for all devices
   *
   * patchData('json_format_version[0]', 2)    // same result
   *
   * @param path The path to the attribute
   * @param newValue The new value
   */
  const patchData = (path: string, newValue: any): any => {
    return _.reduce(
      _.cloneDeep(SMART_DATA_HDD_VERSION_1_0),
      (result: object, dataObj, deviceId) => {
        result[deviceId] = _.set<any>(dataObj, path, newValue);
        return result;
      },
      {}
    );
  };

  /**
   * Initializes the component after it spied upon the `getSmartData()` method
   * of `OsdService`. Determines which data is returned.
   */
  const initializeComponentWithData = (
    dataType: 'hdd_v1' | 'nvme_v1',
    patch: { [path: string]: any } = null,
    simpleChanges?: SimpleChanges
  ) => {
    let data: HddSmartDataV1 | NvmeSmartDataV1;
    switch (dataType) {
      case 'hdd_v1':
        data = SMART_DATA_HDD_VERSION_1_0;
        break;
      case 'nvme_v1':
        data = SMART_DATA_NVME_VERSION_1_0;
        break;
    }

    if (_.isObject(patch)) {
      _.each(patch, (replacement, path) => {
        data = patchData(path, replacement);
      });
    }

    spyOn(osdService, 'getSmartData').and.callFake(() => of(data));
    component.ngOnInit();
    const changes: SimpleChanges = simpleChanges || {
      osdId: new SimpleChange(null, 0, true)
    };
    component.ngOnChanges(changes);
  };

  configureTestBed({
    declarations: [SmartListComponent],
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, NgbNavModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SmartListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    osdService = TestBed.inject(OsdService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('tests HDD version 1.x', () => {
    beforeEach(() => initializeComponentWithData('hdd_v1'));

    it('should return with proper keys', () => {
      _.each(component.data, (smartData, _deviceId) => {
        expect(_.keys(smartData)).toEqual(['info', 'smart', 'device', 'identifier']);
      });
    });

    it('should not contain excluded keys in `info`', () => {
      const excludes = [
        'ata_smart_attributes',
        'ata_smart_selective_self_test_log',
        'ata_smart_data'
      ];
      _.each(component.data, (smartData: SmartDataResult, _deviceId) => {
        _.each(excludes, (exclude) => expect(smartData.info[exclude]).toBeUndefined());
      });
    });
  });

  describe('tests NVMe version 1.x', () => {
    beforeEach(() => initializeComponentWithData('nvme_v1'));

    it('should return with proper keys', () => {
      _.each(component.data, (smartData, _deviceId) => {
        expect(_.keys(smartData)).toEqual(['info', 'smart', 'device', 'identifier']);
      });
    });

    it('should not contain excluded keys in `info`', () => {
      const excludes = ['nvme_smart_health_information_log'];
      _.each(component.data, (smartData: SmartDataResult, _deviceId) => {
        _.each(excludes, (exclude) => expect(smartData.info[exclude]).toBeUndefined());
      });
    });
  });

  it('should not work for version 2.x', () => {
    initializeComponentWithData('nvme_v1', { json_format_version: [2, 0] });
    expect(component.data).toEqual({});
    expect(component.incompatible).toBeTruthy();
  });

  it('should display info panel for passed self test', () => {
    initializeComponentWithData('hdd_v1');
    fixture.detectChanges();
    const alertPanel = fixture.debugElement.query(By.css('cd-alert-panel#alert-self-test-passed'));
    expect(component.incompatible).toBe(false);
    expect(component.loading).toBe(false);
    expect(alertPanel.attributes.size).toBe('slim');
    expect(alertPanel.attributes.title).toBe('SMART overall-health self-assessment test result');
    expect(alertPanel.attributes.type).toBe('info');
  });

  it('should display warning panel for failed self test', () => {
    initializeComponentWithData('hdd_v1', { 'smart_status.passed': false });
    fixture.detectChanges();
    const alertPanel = fixture.debugElement.query(By.css('cd-alert-panel#alert-self-test-failed'));
    expect(component.incompatible).toBe(false);
    expect(component.loading).toBe(false);
    expect(alertPanel.attributes.size).toBe('slim');
    expect(alertPanel.attributes.title).toBe('SMART overall-health self-assessment test result');
    expect(alertPanel.attributes.type).toBe('warning');
  });
});
