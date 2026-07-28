import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { CdTableServerSideService } from './cd-table-server-side.service';

describe('CdTableServerSideService', () => {
  let service: CdTableServerSideService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(CdTableServerSideService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
