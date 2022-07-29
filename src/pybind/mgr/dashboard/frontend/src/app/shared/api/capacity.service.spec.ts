import { TestBed } from '@angular/core/testing';

import { CapacityService } from './capacity.service';

describe('CapacityService', () => {
  let service: CapacityService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CapacityService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
