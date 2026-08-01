import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import {
  MgrModuleResourceStateService,
  MgrModuleResourceState
} from './mgr-module-resource-state.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { MgrModuleInfo } from '~/app/shared/models/mgr-modules.interface';

describe('MgrModuleResourceStateService', () => {
  let service: MgrModuleResourceStateService;
  let mgrModuleServiceMock: { list: jest.Mock; getConfig: jest.Mock };

  // Updated mock to match the MgrModuleInfo interface
  const mockModules: MgrModuleInfo[] = [
    { name: 'dashboard', enabled: true, always_on: true, options: {} },
    { name: 'prometheus', enabled: true, always_on: false, options: {} },
    { name: 'test module', enabled: false, always_on: false, options: {} }
  ];

  const mockConfig = { someKey: 'someValue', enabled: true };

  beforeEach(() => {
    // Set up the API mocks
    mgrModuleServiceMock = {
      list: jest.fn().mockReturnValue(of(mockModules)),
      getConfig: jest.fn().mockReturnValue(of(mockConfig))
    };

    TestBed.configureTestingModule({
      providers: [
        MgrModuleResourceStateService,
        { provide: MgrModuleService, useValue: mgrModuleServiceMock }
      ]
    });

    service = TestBed.inject(MgrModuleResourceStateService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('load()', () => {
    it('should emit null if moduleNameRoute is empty', (done) => {
      service.load('');

      service.state$.pipe(take(1)).subscribe((state) => {
        expect(state).toBeNull();
        expect(mgrModuleServiceMock.list).not.toHaveBeenCalled();
        done();
      });
    });

    it('should fetch data and emit combined state for a valid module', (done) => {
      service.load('dashboard');

      service.state$.pipe(take(1)).subscribe((state) => {
        const expectedState: MgrModuleResourceState = {
          moduleNameRoute: 'dashboard',
          moduleName: 'dashboard',
          moduleInfo: mockModules[0],
          moduleConfig: mockConfig
        };

        expect(state).toEqual(expectedState);
        expect(mgrModuleServiceMock.list).toHaveBeenCalled();
        expect(mgrModuleServiceMock.getConfig).toHaveBeenCalledWith('dashboard');
        done();
      });
    });

    it('should decode URI encoded module names before fetching', (done) => {
      service.load('test%20module'); // URL encoded "test module"

      service.state$.pipe(take(1)).subscribe((state) => {
        expect(mgrModuleServiceMock.getConfig).toHaveBeenCalledWith('test module');
        expect(state?.moduleName).toBe('test module');
        expect(state?.moduleNameRoute).toBe('test%20module');
        done();
      });
    });

    it('should handle malformed URI components gracefully by falling back to the raw string', (done) => {
      // '%' alone will throw a URIError when passed to decodeURIComponent
      const malformedRoute = '%';
      service.load(malformedRoute);

      service.state$.pipe(take(1)).subscribe((state) => {
        expect(mgrModuleServiceMock.getConfig).toHaveBeenCalledWith('%');
        // It should end up emitting null because '%' isn't in our mockModules list
        expect(state).toBeNull();
        done();
      });
    });

    it('should emit null if the decoded module is not found in the API list', (done) => {
      service.load('non_existent_module');

      service.state$.pipe(take(1)).subscribe((state) => {
        expect(state).toBeNull();
        done();
      });
    });

    it('should emit null if the API call throws an error', (done) => {
      // Simulate an API failure
      mgrModuleServiceMock.getConfig.mockReturnValue(throwError(() => new Error('API Error')));

      service.load('dashboard');

      service.state$.pipe(take(1)).subscribe((state) => {
        expect(state).toBeNull();
        done();
      });
    });
  });

  describe('Caching', () => {
    it('should cache the modules list and only call list() once across multiple loads', (done) => {
      service.load('dashboard');

      service.state$.pipe(take(1)).subscribe(() => {
        // First load completes
        expect(mgrModuleServiceMock.list).toHaveBeenCalledTimes(1);

        // Trigger a second load for a different module
        service.load('prometheus');

        service.state$.pipe(take(1)).subscribe((secondState) => {
          expect(secondState?.moduleName).toBe('prometheus');
          // list() should STILL only be called once, because the cache hit returned of(this.modulesCache)
          expect(mgrModuleServiceMock.list).toHaveBeenCalledTimes(1);
          // getConfig is specific to the module, so it should be called twice
          expect(mgrModuleServiceMock.getConfig).toHaveBeenCalledTimes(2);
          done();
        });
      });
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from any pending load requests', () => {
      // Peek into the private loadSub property
      const unsubscribeSpy = jest.spyOn((service as any).loadSub, 'unsubscribe');

      service.ngOnDestroy();

      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
