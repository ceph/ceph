import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { SharedModule } from '../shared.module';
import { configureTestBed } from '../unit-test-helper';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  configureTestBed({
    providers: [SummaryService],
    imports: [HttpClientTestingModule, SharedModule]
  });

  it(
    'should be created',
    inject([SummaryService], (service: SummaryService) => {
      expect(service).toBeTruthy();
    })
  );
});
