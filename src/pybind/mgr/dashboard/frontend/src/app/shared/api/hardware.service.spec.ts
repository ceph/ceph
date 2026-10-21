import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { AuthStorageService } from '../services/auth-storage.service';
import { CompressionStats } from '../enum/hardware.enum';
import { HardwareService } from './hardware.service';
import { MgrModuleService } from './mgr-module.service';

const COMPRESSION_STATS: CompressionStats = {
  fcm_drive_count: 4,
  phy_util_percent: 38.5,
  log_util_percent: 61.2,
  savings_bytes: 7696581394432,
  compression_ratio: 2.4,
  efficiency_percent: 58.3
};

describe('HardwareService', () => {
  let httpTesting: HttpTestingController;
  const getConfig = jest.fn();

  const createService = (canReadConfig = true): HardwareService => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: MgrModuleService, useValue: { getConfig } },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: () => ({ configOpt: { read: canReadConfig } }) }
        }
      ]
    });
    httpTesting = TestBed.inject(HttpTestingController);
    return TestBed.inject(HardwareService);
  };

  beforeEach(() => {
    getConfig.mockReset().mockReturnValue(of({}));
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(createService()).toBeTruthy();
  });

  describe('getCompression', () => {
    it('should request the compression endpoint with the versioned Accept header', () => {
      const service = createService();
      let result: CompressionStats | undefined;

      service.getCompression().subscribe((stats) => (result = stats));

      const req = httpTesting.expectOne('api/hardware/compression');
      expect(req.request.method).toBe('GET');
      expect(req.request.headers.get('Accept')).toBe('application/vnd.ceph.api.v0.1+json');
      req.flush(COMPRESSION_STATS);
      expect(result).toEqual(COMPRESSION_STATS);
    });
  });

  describe('isHardwareMonitoringEnabled$', () => {
    it('should be true when cephadm hw_monitoring is enabled', async () => {
      getConfig.mockReturnValue(of({ hw_monitoring: true }));

      const service = createService();

      await expect(service.isHardwareMonitoringEnabled$.pipe(take(1)).toPromise()).resolves.toBe(
        true
      );
      expect(getConfig).toHaveBeenCalledWith('cephadm');
    });

    it('should be false when cephadm hw_monitoring is disabled', async () => {
      getConfig.mockReturnValue(of({ hw_monitoring: false }));

      const service = createService();

      await expect(service.isHardwareMonitoringEnabled$.pipe(take(1)).toPromise()).resolves.toBe(
        false
      );
    });

    it('should be false when the user cannot read config options', async () => {
      const service = createService(false);

      await expect(service.isHardwareMonitoringEnabled$.pipe(take(1)).toPromise()).resolves.toBe(
        false
      );
      expect(getConfig).not.toHaveBeenCalled();
    });

    it('should be false when cephadm config cannot be fetched', async () => {
      getConfig.mockReturnValue(throwError(() => new Error('cephadm unavailable')));

      const service = createService();

      await expect(service.isHardwareMonitoringEnabled$.pipe(take(1)).toPromise()).resolves.toBe(
        false
      );
    });
  });
});
