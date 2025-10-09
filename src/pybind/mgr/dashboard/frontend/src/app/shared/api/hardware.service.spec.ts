import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { HardwareService } from './hardware.service';

describe('HardwareService', () => {
  let service: HardwareService;

  configureTestBed({
    providers: [HardwareService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(HardwareService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
