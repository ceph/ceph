import { TestBed } from '@angular/core/testing';

import { MulticlusterService } from './multicluster.service';

describe('MulticlusterService', () => {
  let service: MulticlusterService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(MulticlusterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
