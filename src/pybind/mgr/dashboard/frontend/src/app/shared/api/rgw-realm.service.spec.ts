import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

import { RgwRealmService } from './rgw-realm.service';

describe('RgwRealmService', () => {
  let service: RgwRealmService;

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwRealmService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
