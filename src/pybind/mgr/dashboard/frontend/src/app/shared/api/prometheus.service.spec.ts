import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { AlertmanagerNotification } from '../models/prometheus-alerts';
import { PrometheusService } from './prometheus.service';
import { SettingsService } from './settings.service';
import moment from 'moment';

describe('PrometheusService', () => {
  let service: PrometheusService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [PrometheusService, SettingsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(PrometheusService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get alerts', () => {
    service.getGroupedAlerts().subscribe();
    const req = httpTesting.expectOne('api/prometheus/alertgroup?cluster_filter=false');
    expect(req.request.method).toBe('GET');
  });

  it('should get silences', () => {
    service.getSilences().subscribe();
    const req = httpTesting.expectOne('api/prometheus/silences');
    expect(req.request.method).toBe('GET');
  });

  it('should set a silence', () => {
    const silence = {
      id: 'someId',
      matchers: [
        {
          name: 'getZero',
          value: 0,
          isRegex: false
        }
      ],
      startsAt: '2019-01-25T14:32:46.646300974Z',
      endsAt: '2019-01-25T18:32:46.646300974Z',
      createdBy: 'someCreator',
      comment: 'for testing purpose'
    };
    service.setSilence(silence).subscribe();
    const req = httpTesting.expectOne('api/prometheus/silence');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(silence);
  });

  it('should expire a silence', () => {
    service.expireSilence('someId').subscribe();
    const req = httpTesting.expectOne('api/prometheus/silence/someId');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getNotificationSince without a notification', () => {
    service.getNotifications().subscribe();
    const req = httpTesting.expectOne('api/prometheus/notifications?from=last');
    expect(req.request.method).toBe('GET');
  });

  it('should call getNotificationSince with notification', () => {
    service.getNotifications({ id: '42' } as AlertmanagerNotification).subscribe();
    const req = httpTesting.expectOne('api/prometheus/notifications?from=42');
    expect(req.request.method).toBe('GET');
  });

  describe('test getRules()', () => {
    let data: {}; // Subset of PrometheusRuleGroup to keep the tests concise.

    beforeEach(() => {
      data = {
        groups: [
          {
            name: 'test',
            rules: [
              {
                name: 'load_0',
                type: 'alerting'
              },
              {
                name: 'load_1',
                type: 'alerting'
              },
              {
                name: 'load_2',
                type: 'alerting'
              }
            ]
          },
          {
            name: 'recording_rule',
            rules: [
              {
                name: 'node_memory_MemUsed_percent',
                type: 'recording'
              }
            ]
          }
        ]
      };
    });

    it('should get rules without applying filters', () => {
      service.getRules().subscribe((rules) => {
        expect(rules).toEqual(data);
      });

      const req = httpTesting.expectOne('api/prometheus/rules');
      expect(req.request.method).toBe('GET');
      req.flush(data);
    });

    it('should get rewrite rules only', () => {
      service.getRules('rewrites').subscribe((rules) => {
        expect(rules).toEqual({
          groups: [
            { name: 'test', rules: [] },
            { name: 'recording_rule', rules: [] }
          ]
        });
      });

      const req = httpTesting.expectOne('api/prometheus/rules');
      expect(req.request.method).toBe('GET');
      req.flush(data);
    });

    it('should get alerting rules only', () => {
      service.getRules('alerting').subscribe((rules) => {
        expect(rules).toEqual({
          groups: [
            {
              name: 'test',
              rules: [
                { name: 'load_0', type: 'alerting' },
                { name: 'load_1', type: 'alerting' },
                { name: 'load_2', type: 'alerting' }
              ]
            },
            { name: 'recording_rule', rules: [] }
          ]
        });
      });

      const req = httpTesting.expectOne('api/prometheus/rules');
      expect(req.request.method).toBe('GET');
      req.flush(data);
    });
  });

  describe('ifAlertmanagerConfigured', () => {
    let x: any;
    let host: string;

    const receiveConfig = () => {
      const req = httpTesting.expectOne('ui-api/prometheus/alertmanager-api-host');
      expect(req.request.method).toBe('GET');
      req.flush({ value: host });
    };

    beforeEach(() => {
      x = false;
      TestBed.inject(SettingsService)['settings'] = {};
      service.ifAlertmanagerConfigured(
        (v) => (x = v),
        () => (x = [])
      );
      host = 'http://localhost:9093';
    });

    it('changes x in a valid case', () => {
      expect(x).toBe(false);
      receiveConfig();
      expect(x).toBe(host);
    });

    it('does changes x an empty array in a invalid case', () => {
      host = '';
      receiveConfig();
      expect(x).toEqual([]);
    });

    it('disables the set setting', () => {
      receiveConfig();
      service.disableAlertmanagerConfig();
      x = false;
      service.ifAlertmanagerConfigured((v) => (x = v));
      expect(x).toBe(false);
    });
  });

  describe('ifPrometheusConfigured', () => {
    let x: any;
    let host: string;

    const receiveConfig = () => {
      const req = httpTesting.expectOne('ui-api/prometheus/prometheus-api-host');
      expect(req.request.method).toBe('GET');
      req.flush({ value: host });
    };

    beforeEach(() => {
      x = false;
      TestBed.inject(SettingsService)['settings'] = {};
      service.ifPrometheusConfigured(
        (v) => (x = v),
        () => (x = [])
      );
      host = 'http://localhost:9090';
    });

    it('changes x in a valid case', () => {
      expect(x).toBe(false);
      receiveConfig();
      expect(x).toBe(host);
    });

    it('does changes x an empty array in a invalid case', () => {
      host = '';
      receiveConfig();
      expect(x).toEqual([]);
    });

    it('disables the set setting', () => {
      receiveConfig();
      service.disablePrometheusConfig();
      x = false;
      service.ifPrometheusConfigured((v) => (x = v));
      expect(x).toBe(false);
    });
  });

  describe('updateTimeStamp', () => {
    it('should update timestamp correctly', () => {
      const currentTime = moment().unix();
      const selectedTime = {
        start: currentTime - 3600,
        end: currentTime,
        step: 14
      };

      const result = (service as any).updateTimeStamp(selectedTime);

      expect(result).toBeDefined();
      expect(result.step).toBe(14);
      expect(result.start).toBeLessThanOrEqual(currentTime);
      expect(result.end).toBeGreaterThanOrEqual(currentTime);
      expect(result.end - result.start).toBe(3600);
    });
  });

  describe('getMultiClusterData', () => {
    it('should make GET request to correct endpoint', () => {
      const params = { params: 'test_query', start: 123456, end: 123789, step: 14 };
      service.getMultiClusterData(params).subscribe();

      const req = httpTesting.expectOne((request) => {
        return request.url === 'api/prometheus/prometheus_query_data' && request.method === 'GET';
      });
      expect(req.request.params.get('params')).toBe('test_query');
      expect(req.request.params.get('start')).toBe('123456');
      expect(req.request.params.get('end')).toBe('123789');
      expect(req.request.params.get('step')).toBe('14');
      req.flush({ result: [] });
    });
  });

  describe('getMultiClusterQueryRangeData', () => {
    it('should make GET request to correct endpoint', () => {
      const params = { params: 'test_query', start: 123456, end: 123789, step: 14 };
      service.getMultiClusterQueryRangeData(params).subscribe();

      const req = httpTesting.expectOne((request) => {
        return request.url === 'api/prometheus/data' && request.method === 'GET';
      });
      expect(req.request.params.get('params')).toBe('test_query');
      expect(req.request.params.get('start')).toBe('123456');
      expect(req.request.params.get('end')).toBe('123789');
      expect(req.request.params.get('step')).toBe('14');
      req.flush({ result: [] });
    });
  });

  describe('getMultiClusterQueriesData', () => {
    beforeEach(() => {
      spyOn(service, 'ifPrometheusConfigured').and.callFake((fn) => fn());
      service.timerTime = 100; // Reduce timer for faster tests
    });

    afterEach(() => {
      service.unsubscribe();
    });
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
