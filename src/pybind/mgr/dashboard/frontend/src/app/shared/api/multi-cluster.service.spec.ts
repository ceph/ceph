import { TestBed } from '@angular/core/testing';

import { MultiClusterService } from './multi-cluster.service';

describe('MultiClusterService', () => {
  let service: MultiClusterService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(MultiClusterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
