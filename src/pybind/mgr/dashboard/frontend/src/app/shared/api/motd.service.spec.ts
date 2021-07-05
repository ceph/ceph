import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { MotdService } from './motd.service';

describe('MotdService', () => {
  let service: MotdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [MotdService]
  });

  beforeEach(() => {
    service = TestBed.get(MotdService);
    httpTesting = TestBed.get(HttpTestingController);
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
