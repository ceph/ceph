import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { configureTestBed } from '~/testing/unit-test-helper';

import { OverviewStorageService } from './storage-overview.service';
import { PrometheusService } from './prometheus.service';
import { FormatterService } from '../services/formatter.service';

describe('OverviewStorageService', () => {
  let service: OverviewStorageService;

  const prometheusServiceMock = {
    getRangeQueriesData: jest.fn(),
    getPrometheusQueryData: jest.fn(),
    getGaugeQueryData: jest.fn(),
    formatGuageMetric: jest.fn()
  };

  const formatterServiceMock = {
    formatToBinary: jest.fn(),
    convertToUnit: jest.fn()
  };

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [
      { provide: PrometheusService, useValue: prometheusServiceMock },
      { provide: FormatterService, useValue: formatterServiceMock }
    ]
  });

  beforeEach(() => {
    jest.clearAllMocks();
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
        { TOTAL_RAW_USED: 'sum(ceph_osd_stat_bytes_used)' },
        true
      );
    });
  });

  describe('getAverageConsumption', () => {
    it('should format bytes per day correctly', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '1073741824'] }] }) as any);
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['1.0', 'GiB'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('1.0 GiB/day');
        done();
      });
    });

    it('should return 0 formatted when no result', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [] }) as any);
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['0', 'B'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('0 B/day');
        done();
      });
    });

    it('should handle null response gracefully', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(of(null) as any);
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['0', 'B'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('0 B/day');
        done();
      });
    });
  });

  describe('getTimeUntilFull', () => {
    it('should return N/A when days is Infinity', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('N/A');
        done();
      });
    });

    it('should return hours when days < 1', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '0.5'] }] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('12.0 hours');
        done();
      });
    });

    it('should return days when 1 <= days < 30', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '15'] }] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('15.0 days');
        done();
      });
    });

    it('should return months when days >= 30 and < 365', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '60'] }] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('2.0 months');
        done();
      });
    });

    it('should return years when days >= 365', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '730'] }] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('2.0 years');
        done();
      });
    });

    it('should return N/A when days <= 0', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '-5'] }] }) as any);

      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe('N/A');
        done();
      });
    });
  });

  describe('getTopPools', () => {
    it('should map pool results with name', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        of({
          result: [{ metric: { name: 'mypool' }, value: [null, '0.5'] }]
        }) as any
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'mypool', value: 50 }]);
        done();
      });
    });

    it('should fallback to pool label when name is absent', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        of({
          result: [{ metric: { pool: 'fallback_pool' }, value: [null, '0.25'] }]
        }) as any
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'fallback_pool', value: 25 }]);
        done();
      });
    });

    it('should use unknown when no name or pool label', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(
        of({
          result: [{ metric: {}, value: [null, '0.1'] }]
        }) as any
      );

      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'unknown', value: 10 }]);
        done();
      });
    });

    it('should return empty array when result is empty', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [] }) as any);

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
        .mockReturnValue(of({ result: [{ value: [null, '42'] }] }) as any);

      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(42);
        done();
      });
    });

    it('should return 0 when result is empty', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [] }) as any);

      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(0);
        done();
      });
    });

    it('should return 0 when response is null', (done) => {
      jest.spyOn(service['prom'], 'getPrometheusQueryData').mockReturnValue(of(null) as any);

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
        .mockReturnValue(of({ result: [{ value: [null, '3'] }] }) as any);

      const mockRgwService = {
        getTotalBucketsAndUsersLength: () => of({ buckets_count: 10 })
      };

      service.getObjectCounts(mockRgwService).subscribe((result) => {
        expect(result).toEqual({ buckets: 10, pools: 3 });
        done();
      });
    });

    it('should default buckets to 0 when buckets_count is missing', (done) => {
      jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({ result: [{ value: [null, '2'] }] }) as any);

      const mockRgwService = {
        getTotalBucketsAndUsersLength: () => of({})
      };

      service.getObjectCounts(mockRgwService).subscribe((result) => {
        expect(result).toEqual({ buckets: 0, pools: 2 });
        done();
      });
    });
  });

  describe('getStorageBreakdown', () => {
    it('should call getPrometheusQueryData with storage breakdown query', () => {
      const promSpy = jest
        .spyOn(service['prom'], 'getPrometheusQueryData')
        .mockReturnValue(of({}) as any);

      service.getStorageBreakdown().subscribe();

      expect(promSpy).toHaveBeenCalledWith({
        params:
          'sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})'
      });
    });
  });

  describe('formatBytesForChart', () => {
    it('should delegate to formatter.formatToBinary', () => {
      const formatterSpy = jest
        .spyOn(service['formatter'], 'formatToBinary')
        .mockReturnValue([3, 'GiB'] as any);

      const result = service.formatBytesForChart(3221225472);

      expect(formatterSpy).toHaveBeenCalledWith(3221225472, true);
      expect(result).toEqual([3, 'GiB']);
    });
  });

  describe('convertBytesToUnit', () => {
    it('should delegate to formatter.convertToUnit', () => {
      const formatterSpy = jest.spyOn(service['formatter'], 'convertToUnit').mockReturnValue(12.5);

      const result = service.convertBytesToUnit(13421772800, 'GiB');

      expect(formatterSpy).toHaveBeenCalledWith(13421772800, 'B', 'GiB', 1);
      expect(result).toBe(12.5);
    });
  });

  describe('mapStorageChartData', () => {
    it('should map Block, Filesystem, and Object groups', () => {
      jest
        .spyOn(service, 'convertBytesToUnit')
        .mockImplementation((value: number) => Number(value));

      const result = service.mapStorageChartData(
        {
          result: [
            { metric: { application: 'Block' }, value: [0, '100'] },
            { metric: { application: 'Filesystem' }, value: [0, '200'] },
            { metric: { application: 'Object' }, value: [0, '300'] }
          ]
        } as any,
        'B',
        600
      );

      expect(result).toEqual([
        { group: 'Block', value: 100 },
        { group: 'File system', value: 200 },
        { group: 'Object', value: 300 }
      ]);
    });

    it('should add System metadata for unassigned bytes', () => {
      jest
        .spyOn(service, 'convertBytesToUnit')
        .mockImplementation((value: string | number) => Number(value));

      const result = service.mapStorageChartData(
        {
          result: [
            { metric: { application: 'Block' }, value: [0, '100'] },
            { metric: { application: 'Filesystem' }, value: [0, '200'] }
          ]
        } as any,
        'B',
        500
      );

      expect(result).toEqual([
        { group: 'Block', value: 100 },
        { group: 'File system', value: 200 },
        { group: 'System metadata', value: 200 }
      ]);
    });

    it('should treat unknown application bytes as system metadata', () => {
      jest
        .spyOn(service, 'convertBytesToUnit')
        .mockImplementation((value: string | number) => Number(value));

      const result = service.mapStorageChartData(
        {
          result: [
            { metric: { application: 'Unknown' }, value: [0, '50'] },
            { metric: { application: 'Block' }, value: [0, '100'] }
          ]
        } as any,
        'B',
        150
      );

      expect(result).toEqual([
        { group: 'Block', value: 100 },
        { group: 'System metadata', value: 50 }
      ]);
    });

    it('should return empty array when unit is missing', () => {
      const result = service.mapStorageChartData({ result: [] } as any, '', 100);
      expect(result).toEqual([]);
    });

    it('should return empty array when data is null', () => {
      const result = service.mapStorageChartData(null as any, 'B', 100);
      expect(result).toEqual([]);
    });

    it('should return empty array when totalUsedBytes is null', () => {
      const result = service.mapStorageChartData({ result: [] } as any, 'B', null as any);
      expect(result).toEqual([]);
    });

    it('should filter out zero-value converted entries', () => {
      jest
        .spyOn(service, 'convertBytesToUnit')
        .mockImplementation((value: string | number) => Number(value));
      const result = service.mapStorageChartData(
        {
          result: [
            { metric: { application: 'mgr' }, value: [0, '50'] },
            { metric: { application: 'Object' }, value: [0, '0'] },
            { metric: { application: 'Block' }, value: [0, '0'] }
          ]
        } as any,
        'B',
        50
      );

      expect(result).toEqual([{ group: 'System metadata', value: 50 }]);
    });
  });
});
