import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

import { OverviewStorageService } from './storage-overview.service';

describe('OverviewStorageService', () => {
  let service: OverviewStorageService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(OverviewStorageService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('getTrendData', () => {
    it('should call getRangeQueriesData with correct params', () => {
      const promSpy = jest.spyOn(service['prom'], 'getRangeQueriesData').mockReturnValue({} as any);
      service.getTrendData(1000, 2000, 60);
      expect(promSpy).toHaveBeenCalledWith(
        { start: 1000, end: 2000, step: 60 },
        { TOTAL_RAW_USED: 'sum(ceph_pool_bytes_used)' },
        true
      );
    });
  });

  describe('getAverageConsumption', () => {
    it('should format bytes per day correctly', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '1073741824'] }] }));
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['1.0', 'GiB'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('1.0 GiB/day');
        done();
      });
    });

    it('should return 0 formatted when no result', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [] }));
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['0', 'B'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('0 B/day');
        done();
      });
    });

    it('should handle null response gracefully', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)(null));
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['0', 'B'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('0 B/day');
        done();
      });
    });
  });

  describe('getTimeUntilFull', () => {
    it('should return ∞ when days is Infinity', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [] }));

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('∞');
        done();
      });
    });

    it('should return hours when days < 1', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '0.5'] }] }));

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('12.0 hours');
        done();
      });
    });

    it('should return days when 1 <= days < 30', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '15'] }] }));

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('15.0 days');
        done();
      });
    });

    it('should return months when days >= 30', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '60'] }] }));

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('2.0 months');
        done();
      });
    });

    it('should return ∞ when days <= 0', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '-5'] }] }));

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('∞');
        done();
      });
    });
  });

  describe('getTopPools', () => {
    it('should map pool results with name', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        new (require('rxjs').of)({
          result: [{ metric: { name: 'mypool' }, value: [null, '0.5'] }]
        })
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'mypool', value: 50 }]);
        done();
      });
    });

    it('should fallback to pool label when name is absent', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        new (require('rxjs').of)({
          result: [{ metric: { pool: 'fallback_pool' }, value: [null, '0.25'] }]
        })
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'fallback_pool', value: 25 }]);
        done();
      });
    });

    it('should use "unknown" when no name or pool label', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        new (require('rxjs').of)({
          result: [{ metric: {}, value: [null, '0.1'] }]
        })
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'unknown', value: 10 }]);
        done();
      });
    });

    it('should return empty array when result is empty', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [] }));

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([]);
        done();
      });
    });
  });

  describe('getCount', () => {
    it('should return numeric count from query result', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '42'] }] }));

      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(42);
        done();
      });
    });

    it('should return 0 when result is empty', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [] }));

      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(0);
        done();
      });
    });

    it('should return 0 when response is null', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)(null));

      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(0);
        done();
      });
    });
  });

  describe('getObjectCounts', () => {
    it('should return bucket and pool counts', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '3'] }] }));

      const mockRgwService = {
        getTotalBucketsAndUsersLength: () => new (require('rxjs').of)({ buckets_count: 10 })
      };

      service.getObjectCounts(mockRgwService).subscribe((result) => {
        expect(result).toEqual({ buckets: 10, pools: 3 });
        done();
      });
    });

    it('should default buckets to 0 when buckets_count is missing', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(new (require('rxjs').of)({ result: [{ value: [null, '2'] }] }));

      const mockRgwService = {
        getTotalBucketsAndUsersLength: () => new (require('rxjs').of)({})
      };

      service.getObjectCounts(mockRgwService).subscribe((result) => {
        expect(result).toEqual({ buckets: 0, pools: 2 });
        done();
      });
    });
  });
});
