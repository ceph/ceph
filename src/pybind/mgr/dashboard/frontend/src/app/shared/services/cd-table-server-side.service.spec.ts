/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { CdTableServerSideService } from './cd-table-server-side.service';

describe('Service: CdTableServerSide', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [CdTableServerSideService]
    });
  });

  it('should ...', inject([CdTableServerSideService], (service: CdTableServerSideService) => {
    expect(service).toBeTruthy();
  }));
});
