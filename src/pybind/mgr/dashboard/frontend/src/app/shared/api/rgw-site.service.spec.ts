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
    service = TestBed.inject(RgwSiteService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should contain site endpoint in GET request', () => {
    service.get().subscribe();
    const req = httpTesting.expectOne(service['url']);
    expect(req.request.method).toBe('GET');
  });

  it('should add query param in GET request', () => {
    const query = 'placement-targets';
    service.get(query).subscribe();
    httpTesting.expectOne(`${service['url']}?query=placement-targets`);
  });
});
