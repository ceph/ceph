import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { SharedModule } from '../shared.module';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SummaryService],
      imports: [HttpClientTestingModule, SharedModule]
    });
  });

  it(
    'should be created',
    inject([SummaryService], (service: SummaryService) => {
      expect(service).toBeTruthy();
    })
  );
});
