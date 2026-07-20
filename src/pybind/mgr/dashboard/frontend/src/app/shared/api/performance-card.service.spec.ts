import { TestBed } from '@angular/core/testing';

import { PerformanceCardService } from './performance-card.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('PerformanceCardService', () => {
  let service: PerformanceCardService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(PerformanceCardService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('convertPerformanceData', () => {
    it('should convert raw performance data correctly', () => {
      const raw = {
        READIOPS: [
          [1609459200, '100'],
          [1609459260, '200']
        ],
        WRITEIOPS: [
          [1609459200, '50'],
          [1609459260, '75']
        ],
        READLATENCY: [
          [1609459200, '1.5'],
          [1609459260, '2.0']
        ],
        WRITELATENCY: [
          [1609459200, '2.5'],
          [1609459260, '3.0']
        ],
        READCLIENTTHROUGHPUT: [
          [1609459200, '1000'],
          [1609459260, '2000']
        ],
        WRITECLIENTTHROUGHPUT: [
          [1609459200, '500'],
          [1609459260, '750']
        ]
      };

      const result = service.convertPerformanceData(raw);

      expect(result).toBeDefined();
      expect(result.iops).toBeDefined();
      expect(result.latency).toBeDefined();
      expect(result.throughput).toBeDefined();

      // Check iops data
      expect(result.iops.length).toBe(2);
      expect(result.iops[0].values['Read IOPS']).toBe(100);
      expect(result.iops[0].values['Write IOPS']).toBe(50);

      // Check latency data
      expect(result.latency.length).toBe(2);
      expect(result.latency[0].values['Read Latency']).toBe(1.5);
      expect(result.latency[0].values['Write Latency']).toBe(2.5);

      // Check throughput data
      expect(result.throughput.length).toBe(2);
      expect(result.throughput[0].values['Read Throughput']).toBe(1000);
      expect(result.throughput[0].values['Write Throughput']).toBe(500);
    });
  });

  describe('toSeries', () => {
    it('should convert metric array to series format', () => {
      const metric: [number, string][] = [
        [1609459200, '100'],
        [1609459260, '200']
      ];
      const label = 'Test Label';

      const result = (service as any).toSeries(metric, label);

      expect(result.length).toBe(2);
      expect(result[0].timestamp).toEqual(new Date(1609459200 * 1000));
      expect(result[0].values[label]).toBe(100);
      expect(result[1].timestamp).toEqual(new Date(1609459260 * 1000));
      expect(result[1].values[label]).toBe(200);
    });
  });

  describe('mergeSeries', () => {
    it('should merge multiple series into one', () => {
      const series1 = [
        {
          timestamp: new Date(1609459200000),
          values: { 'Series 1': 100 }
        },
        {
          timestamp: new Date(1609459260000),
          values: { 'Series 1': 200 }
        }
      ];
      const series2 = [
        {
          timestamp: new Date(1609459200000),
          values: { 'Series 2': 50 }
        },
        {
          timestamp: new Date(1609459260000),
          values: { 'Series 2': 75 }
        }
      ];

      const result = (service as any).mergeSeries(series1, series2);

      expect(result.length).toBe(2);
      expect(result[0].values['Series 1']).toBe(100);
      expect(result[0].values['Series 2']).toBe(50);
      expect(result[1].values['Series 1']).toBe(200);
      expect(result[1].values['Series 2']).toBe(75);
    });
  });
});
