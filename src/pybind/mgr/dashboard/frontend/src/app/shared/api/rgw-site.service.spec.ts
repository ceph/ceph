import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RgwSiteService } from './rgw-site.service';

describe('RgwSiteService', () => {
  let service: RgwSiteService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RgwSiteService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(RgwSiteService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getPlacementTargets', () => {
    service.getPlacementTargets().subscribe();
    const req = httpTesting.expectOne('api/rgw/site?query=placement-targets');
    expect(req.request.method).toBe('GET');
  });
});
