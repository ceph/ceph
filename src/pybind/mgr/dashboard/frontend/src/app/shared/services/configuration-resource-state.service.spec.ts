import { TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import {
  ConfigurationResourceStateService,
  ConfigurationOption
} from './configuration-resource-state.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';

describe('ConfigurationResourceStateService', () => {
  let service: ConfigurationResourceStateService;
  let configurationServiceMock: { get: jest.Mock };

  // Mock data including all required properties
  const mockConfigOption: ConfigurationOption = {
    name: 'test_config',
    type: 'string',
    level: 'basic',
    desc: 'A test configuration',
    long_desc: 'A longer description for the test configuration',
    default: '',
    daemon_default: '',
    min: '',
    max: '',
    can_update_at_runtime: true,
    value: [{ section: 'global', value: 'test_value' }]
  };

  beforeEach(() => {
    // Set up the API mock
    configurationServiceMock = {
      get: jest.fn()
    };

    TestBed.configureTestingModule({
      providers: [
        ConfigurationResourceStateService,
        { provide: ConfigurationService, useValue: configurationServiceMock }
      ]
    });

    service = TestBed.inject(ConfigurationResourceStateService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('load()', () => {
    it('should emit null if configNameRoute is empty', (done) => {
      service.load('');

      service.configuration$.pipe(take(1)).subscribe((config) => {
        expect(config).toBeNull();
        expect(configurationServiceMock.get).not.toHaveBeenCalled();
        done();
      });
    });

    it('should fetch data from the API and emit the config option', (done) => {
      configurationServiceMock.get.mockReturnValue(of(mockConfigOption));

      service.load('test_config');

      service.configuration$.pipe(take(1)).subscribe((config) => {
        expect(config).toEqual(mockConfigOption);
        expect(configurationServiceMock.get).toHaveBeenCalledWith('test_config');
        done();
      });
    });

    it('should emit null if the API call throws an error', (done) => {
      // Simulate an API failure
      configurationServiceMock.get.mockReturnValue(throwError(() => new Error('API Error')));

      service.load('test_config');

      service.configuration$.pipe(take(1)).subscribe((config) => {
        expect(config).toBeNull();
        expect(configurationServiceMock.get).toHaveBeenCalledWith('test_config');
        done();
      });
    });

    it('should ignore stale responses from previous load calls', () => {
      const firstRequest$ = new Subject<ConfigurationOption>();
      const secondRequest$ = new Subject<ConfigurationOption>();
      const selectedConfigNames: string[] = [];

      configurationServiceMock.get
        .mockReturnValueOnce(firstRequest$)
        .mockReturnValueOnce(secondRequest$);

      service.configuration$.subscribe((config) => {
        if (config?.name) {
          selectedConfigNames.push(config.name);
        }
      });

      service.load('first_config');
      service.load('second_config');

      secondRequest$.next({ ...mockConfigOption, name: 'second_config' });
      expect(selectedConfigNames).toEqual(['second_config']);

      firstRequest$.next({ ...mockConfigOption, name: 'first_config' });
      expect(selectedConfigNames).toEqual(['second_config']);
    });
  });
});
