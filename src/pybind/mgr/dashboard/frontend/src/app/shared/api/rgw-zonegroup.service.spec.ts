import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

import { RgwZonegroupService } from './rgw-zonegroup.service';

describe('RgwZonegroupService', () => {
  let service: RgwZonegroupService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(RgwZonegroupService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
