import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

import { RgwZoneService } from './rgw-zone.service';

describe('RgwZoneService', () => {
  let service: RgwZoneService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwZoneService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
