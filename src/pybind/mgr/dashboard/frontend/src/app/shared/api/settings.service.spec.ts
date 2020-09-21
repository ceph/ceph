import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SettingsService } from './settings.service';

describe('SettingsService', () => {
  let service: SettingsService;
  let httpTesting: HttpTestingController;

  const exampleUrl = 'api/settings/something';
  const exampleValue = 'http://localhost:3000';

  configureTestBed({
    providers: [SettingsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(SettingsService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call validateGrafanaDashboardUrl', () => {
    service.validateGrafanaDashboardUrl('s').subscribe();
    const req = httpTesting.expectOne('api/grafana/validation/s');
    expect(req.request.method).toBe('GET');
  });

  describe('getSettingsValue', () => {
    const testMethod = (data: object, expected: string) => {
      expect(service['getSettingsValue'](data)).toBe(expected);
    };

    it('should explain the logic of the method', () => {
      expect('' || undefined).toBe(undefined);
      expect(undefined || '').toBe('');
      expect('test' || undefined || '').toBe('test');
    });

    it('should test the method for empty string values', () => {
      testMethod({}, '');
      testMethod({ wrongAttribute: 'test' }, '');
      testMethod({ value: '' }, '');
      testMethod({ instance: '' }, '');
    });

    it('should test the method for non empty string values', () => {
      testMethod({ value: 'test' }, 'test');
      testMethod({ instance: 'test' }, 'test');
    });
  });

  describe('isSettingConfigured', () => {
    let increment: number;

    const testConfig = (url: string, value: string) => {
      service.ifSettingConfigured(
        url,
        (setValue) => {
          expect(setValue).toBe(value);
          increment++;
        },
        () => {
          increment--;
        }
      );
    };

    const expectSettingsApiCall = (url: string, value: object, isSet: string) => {
      testConfig(url, isSet);
      const req = httpTesting.expectOne(url);
      expect(req.request.method).toBe('GET');
      req.flush(value);
      tick();
      expect(increment).toBe(isSet !== '' ? 1 : -1);
      expect(service['settings'][url]).toBe(isSet);
    };

    beforeEach(() => {
      increment = 0;
    });

    it(`should return true if 'value' does not contain an empty string`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, { value: exampleValue }, exampleValue);
    }));

    it(`should return false if 'value' does contain an empty string`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, { value: '' }, '');
    }));

    it(`should return true if 'instance' does not contain an empty string`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, { value: exampleValue }, exampleValue);
    }));

    it(`should return false if 'instance' does contain an empty string`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, { instance: '' }, '');
    }));

    it(`should return false if the api object is empty`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, {}, '');
    }));

    it(`should call the API once even if it is called multiple times`, fakeAsync(() => {
      expectSettingsApiCall(exampleUrl, { value: exampleValue }, exampleValue);
      testConfig(exampleUrl, exampleValue);
      httpTesting.expectNone(exampleUrl);
      expect(increment).toBe(2);
    }));
  });

  it('should disable a set setting', () => {
    service['settings'] = { [exampleUrl]: exampleValue };
    service.disableSetting(exampleUrl);
    expect(service['settings']).toEqual({ [exampleUrl]: '' });
  });

  it('should return the specified settings (1)', () => {
    let result;
    service.getValues('foo,bar').subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne('api/settings?names=foo,bar');
    expect(req.request.method).toBe('GET');
    req.flush([
      { name: 'foo', default: '', type: 'str', value: 'test' },
      { name: 'bar', default: 0, type: 'int', value: 2 }
    ]);
    expect(result).toEqual({
      foo: 'test',
      bar: 2
    });
  });

  it('should return the specified settings (2)', () => {
    service.getValues(['abc', 'xyz']).subscribe();
    const req = httpTesting.expectOne('api/settings?names=abc,xyz');
    expect(req.request.method).toBe('GET');
  });

  it('should return standard settings', () => {
    service.getStandardSettings().subscribe();
    const req = httpTesting.expectOne('ui-api/standard_settings');
    expect(req.request.method).toBe('GET');
  });
});
