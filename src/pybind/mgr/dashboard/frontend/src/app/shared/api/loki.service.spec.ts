import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { LokiService } from './loki.service';

describe('LokiService', () => {
  let service: LokiService;
  let httpTesting: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [LokiService]
    });
    service = TestBed.inject(LokiService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
    localStorage.clear();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch label values', () => {
    service.getLabelValues('filename', '{job="Cluster Logs"}', { since: '7d' }).subscribe((values) => {
      expect(values).toEqual(['/var/log/ceph/cephadm.log']);
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.url === 'api/loki/label/filename/values' &&
        request.params.get('params') === '{job="Cluster Logs"}' &&
        request.params.get('since') === '7d'
    );
    expect(req.request.method).toBe('GET');
    req.flush(['/var/log/ceph/cephadm.log']);
  });

  it('should fetch labels', () => {
    service.getLabels(undefined, { since: '24h' }).subscribe((labels) => {
      expect(labels).toEqual(['filename', 'job']);
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.url === 'api/loki/labels' && request.params.get('since') === '24h'
    );
    req.flush(['filename', 'job']);
  });

  it('should run instant query', () => {
    service.query('count_over_time({job="Cluster Logs"}[1h])').subscribe((response) => {
      expect(response.resultType).toBe('vector');
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.url === 'api/loki/query' &&
        request.params.get('params') === 'count_over_time({job="Cluster Logs"}[1h])'
    );
    req.flush({ resultType: 'vector', result: [] });
  });

  it('should format log lines for download', () => {
    const lines = [
      { stamp: '2026-06-15T10:00:00.000Z', message: 'hello', filename: '/var/log/ceph/cephadm.log' }
    ];
    expect(service.logLinesToText(lines)).toBe('2026-06-15T10:00:00.000Z\thello');
    expect(service.downloadFileNameFromPath('/var/log/ceph/cephadm.log')).toBe('cephadm');
  });

  it('should persist and restore view state without log lines', () => {
    const state = {
      selectedFilename: '/var/log/ceph/cephadm.log',
      hasRun: true
    };

    service.saveViewState(state);
    expect(service.getViewState()).toEqual(state);

    service.clearViewState();
    expect(service.getViewState()).toBeNull();
  });

  it('should fetch logs via getLogs', () => {
    service.getLogs('/var/log/ceph/cephadm.log').subscribe((lines) => {
      expect(lines.length).toBe(1);
      expect(lines[0].message).toBe('hello');
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.url === 'api/loki/query_range' &&
        request.params.get('params') ===
          '{job="Cluster Logs", filename="/var/log/ceph/cephadm.log"}' &&
        request.params.get('since') === '24h' &&
        request.params.get('limit') === '5000'
    );
    req.flush({
      resultType: 'streams',
      result: [
        {
          stream: { filename: '/var/log/ceph/cephadm.log', job: 'Cluster Logs' },
          values: [['1740000000000000000', 'hello']]
        }
      ]
    });
  });

  it('should run query_range and parse log lines', () => {
    service
      .getLogLinesForFilename('/var/log/ceph/cephadm.log', { since: '1h', limit: 10 })
      .subscribe((lines) => {
        expect(lines.length).toBe(1);
        expect(lines[0].message).toBe('hello');
        expect(lines[0].filename).toBe('/var/log/ceph/cephadm.log');
      });

    const req = httpTesting.expectOne(
      (request) =>
        request.url === 'api/loki/query_range' &&
        request.params.get('params') ===
          '{job="Cluster Logs", filename="/var/log/ceph/cephadm.log"}' &&
        request.params.get('since') === '1h' &&
        request.params.get('limit') === '10'
    );
    req.flush({
      resultType: 'streams',
      result: [
        {
          stream: { filename: '/var/log/ceph/cephadm.log', job: 'Cluster Logs' },
          values: [['1740000000000000000', 'hello']]
        }
      ]
    });
  });
});
