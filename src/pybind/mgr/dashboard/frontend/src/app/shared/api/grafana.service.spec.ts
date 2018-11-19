import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { GrafanaService } from './grafana.service';

describe('GrafanaService', () => {
  let service: GrafanaService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [GrafanaService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(GrafanaService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get protocol', () => {
    service.getGrafanaApiUrl().subscribe();
    const req = httpTesting.expectOne('api/grafana/url');
    expect(req.request.method).toBe('GET');
  });
});
