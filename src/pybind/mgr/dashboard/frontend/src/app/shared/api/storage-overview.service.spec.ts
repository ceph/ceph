import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { configureTestBed } from '~/testing/unit-test-helper';

import { OverviewStorageService } from './storage-overview.service';

describe('OverviewStorageService', () => {
  let service: OverviewStorageService;
  let httpMock: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(OverviewStorageService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('getTrendData', () => {
    it('should request semantic storage trend data and normalize NaN values', (done) => {
      service.getTrendData(1000, 2000, 60).subscribe((result) => {
        expect(result).toEqual({
          TOTAL_RAW_USED: [
            [1, '10'],
            [2, '0']
          ]
        });
        done();
      });

      const req = httpMock.expectOne(
        'api/prometheus/overview/storage/trend?start=1000&end=2000&step=60'
      );
      expect(req.request.method).toBe('GET');
      req.flush({ total_raw_used: [[1, '10'], [2, 'NaN']] });
    });
  });

  describe('getAverageConsumption', () => {
    it('should format bytes per day correctly', (done) => {
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['1.0', 'GiB'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('1.0 GiB/day');
        done();
      });

      httpMock
        .expectOne('api/prometheus/overview/storage')
        .flush({ average_consumption_per_day: '1073741824' });
    });

    it('should return 0 formatted when no result', (done) => {
      jest.spyOn(service['formatter'], 'formatToBinary').mockReturnValue(['0', 'B'] as any);

      service.getAverageConsumption().subscribe((result) => {
        expect(result).toBe('0 B/day');
        done();
      });

      httpMock.expectOne('api/prometheus/overview/storage').flush({});
    });
  });

  describe('getTimeUntilFull', () => {
    const expectValue = (days: string | null, expected: string, done: DoneFn) => {
      service.getTimeUntilFull().subscribe((result) => {
        expect(result).toBe(expected);
        done();
      });

      httpMock.expectOne('api/prometheus/overview/storage').flush({ time_until_full_days: days });
    };

    it('should return N/A when days is Infinity', (done) => {
      expectValue(null, 'N/A', done);
    });

    it('should return hours when days < 1', (done) => {
      expectValue('0.5', '12.0 hours', done);
    });

    it('should return days when 1 <= days < 30', (done) => {
      expectValue('15', '15.0 days', done);
    });

    it('should return months when days >= 30 and < 365', (done) => {
      expectValue('60', '2.0 months', done);
    });

    it('should return years when days >= 365', (done) => {
      expectValue('730', '2.0 years', done);
    });

    it('should return N/A when days <= 0', (done) => {
      expectValue('-5', 'N/A', done);
    });
  });

  describe('getTopPools', () => {
    it('should map pool results with name', (done) => {
      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'mypool', value: 50 }]);
        done();
      });

      const req = httpMock.expectOne(
        (request) =>
          request.url === 'api/prometheus/prometheus_query_data' &&
          request.params.get('params') === 'some_query'
      );
      req.flush({
        result: [{ metric: { name: 'mypool' }, value: [null, '0.5'] }]
      });
    });

    it('should fallback to pool label when name is absent', (done) => {
      service.getTopPools('some_query').subscribe((result) => {
        expect(result).toEqual([{ group: 'fallback_pool', value: 25 }]);
        done();
      });

      httpMock
        .expectOne('api/prometheus/prometheus_query_data?params=some_query')
        .flush({ result: [{ metric: { pool: 'fallback_pool' }, value: [null, '0.25'] }] });
    });
  });

  describe('getCount', () => {
    it('should return numeric count from query result', (done) => {
      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(42);
        done();
      });

      httpMock
        .expectOne('api/prometheus/prometheus_query_data?params=some_query')
        .flush({ result: [{ value: [null, '42'] }] });
    });

    it('should return 0 when response is empty', (done) => {
      service.getCount('some_query').subscribe((result) => {
        expect(result).toBe(0);
        done();
      });

      httpMock.expectOne('api/prometheus/prometheus_query_data?params=some_query').flush({});
    });
  });

  describe('getObjectCounts', () => {
    it('should return bucket and pool counts', (done) => {
      const mockRgwService = {
        getTotalBucketsAndUsersLength: () => of({ buckets_count: 10 })
      };

      service.getObjectCounts(mockRgwService).subscribe((result) => {
        expect(result).toEqual({ buckets: 10, pools: 3 });
        done();
      });

      httpMock
        .expectOne(
          (request) =>
            request.url === 'api/prometheus/prometheus_query_data' &&
            request.params.get('params') === 'count(ceph_pool_metadata{application="Object"})'
        )
        .flush({ result: [{ value: [null, '3'] }] });
    });
  });

  describe('getRawCapacityThresholds', () => {
    it('should parse ratios from storage insights', (done) => {
      service.getRawCapacityThresholds().subscribe((result) => {
        expect(result).toEqual({
          osdFullRatio: 0.95,
          osdNearfullRatio: 0.85
        });
        done();
      });

      httpMock.expectOne('api/prometheus/overview/storage').flush({
        osd_full_ratio: '0.95',
        osd_nearfull_ratio: '0.85'
      });
    });
  });

  describe('getStorageBreakdown', () => {
    it('should return semantic breakdown data', (done) => {
      service.getStorageBreakdown().subscribe((result) => {
        expect(result).toEqual([{ application: 'Block', value: '123' }]);
        done();
      });

      httpMock.expectOne('api/prometheus/overview/storage').flush({
        breakdown: [{ application: 'Block', value: '123' }]
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
        [
          { application: 'Block', value: '100' },
          { application: 'Filesystem', value: '200' },
          { application: 'Object', value: '300' }
        ],
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
      jest.spyOn(service, 'convertBytesToUnit').mockImplementation((value: number) => Number(value));

      const result = service.mapStorageChartData(
        [
          { application: 'Block', value: '100' },
          { application: 'Filesystem', value: '200' }
        ],
        'B',
        500
      );

      expect(result).toEqual([
        { group: 'Block', value: 100 },
        { group: 'File system', value: 200 },
        { group: 'System metadata', value: 200 }
      ]);
    });
  });
});
