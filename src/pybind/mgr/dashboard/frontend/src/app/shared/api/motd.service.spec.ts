import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { MotdService } from '~/app/shared/api/motd.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('MotdService', () => {
  let service: MotdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [MotdService]
  });

  beforeEach(() => {
    service = TestBed.inject(MotdService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get MOTD', () => {
    service.get().subscribe();
    const req = httpTesting.expectOne('ui-api/motd');
    expect(req.request.method).toBe('GET');
  });
});
