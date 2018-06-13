import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  let service: ConfigurationService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [ConfigurationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(ConfigurationService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getConfigData', () => {
    service.getConfigData().subscribe();
    const req = httpTesting.expectOne('api/cluster_conf/');
    expect(req.request.method).toBe('GET');
  });
});
