import { TestBed } from '@angular/core/testing';

import * as moment from 'moment';

import { CdDevice } from '../models/devices';
import { DeviceService } from './device.service';

describe('DeviceService', () => {
  let service: DeviceService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DeviceService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('should test getDevices pipe', () => {
    let now: jasmine.Spy = null;

    const newDevice = (data: object): CdDevice => {
      const device: CdDevice = {
        devid: '',
        location: [{ host: '', dev: '' }],
        daemons: []
      };
      Object.assign(device, data);
      return device;
    };

    beforeEach(() => {
      // Mock 'moment.now()' to simplify testing by enabling testing with fixed dates.
      now = spyOn(moment, 'now').and.returnValue(
        moment('2019-10-01T00:00:00.00000+0100').valueOf()
      );
    });

    afterEach(() => {
      expect(now).toHaveBeenCalled();
    });

    it('should return status "good" for life expectancy > 6 weeks', () => {
      const preparedDevice = service.calculateAdditionalData(
        newDevice({
          life_expectancy_min: '2019-11-14T01:00:00.000000+0100',
          life_expectancy_max: '0.000000',
          life_expectancy_stamp: '2019-10-01T02:08:48.627312+0100'
        })
      );
      expect(preparedDevice.life_expectancy_weeks).toEqual({ max: null, min: 6 });
      expect(preparedDevice.state).toBe('good');
    });

    it('should return status "warning" for life expectancy <= 4 weeks', () => {
      const preparedDevice = service.calculateAdditionalData(
        newDevice({
          life_expectancy_min: '2019-10-14T01:00:00.000000+0100',
          life_expectancy_max: '2019-11-14T01:00:00.000000+0100',
          life_expectancy_stamp: '2019-10-01T00:00:00.00000+0100'
        })
      );
      expect(preparedDevice.life_expectancy_weeks).toEqual({ max: 6, min: 2 });
      expect(preparedDevice.state).toBe('warning');
    });

    it('should return status "bad" for life expectancy <= 2 weeks', () => {
      const preparedDevice = service.calculateAdditionalData(
        newDevice({
          life_expectancy_min: '0.000000',
          life_expectancy_max: '2019-10-12T01:00:00.000000+0100',
          life_expectancy_stamp: '2019-10-01T00:00:00.00000+0100'
        })
      );
      expect(preparedDevice.life_expectancy_weeks).toEqual({ max: 2, min: null });
      expect(preparedDevice.state).toBe('bad');
    });

    it('should return status "stale" for time stamp that is older than a week', () => {
      const preparedDevice = service.calculateAdditionalData(
        newDevice({
          life_expectancy_min: '0.000000',
          life_expectancy_max: '0.000000',
          life_expectancy_stamp: '2019-09-21T00:00:00.00000+0100'
        })
      );
      expect(preparedDevice.life_expectancy_weeks).toEqual({ max: null, min: null });
      expect(preparedDevice.state).toBe('stale');
    });
  });
});
