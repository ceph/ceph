import { TestBed } from '@angular/core/testing';

import { MultiClusterService } from './multi-cluster.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('MultiClusterService', () => {
  let service: MultiClusterService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(MultiClusterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
