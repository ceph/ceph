import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap';

import { configureTestBed } from '../unit-test-helper';
import { PerformanceCounterService } from './performance-counter.service';

describe('PerformanceCounterService', () => {
  configureTestBed({
    providers: [PerformanceCounterService],
    imports: [HttpClientTestingModule, BsDropdownModule.forRoot(), HttpClientModule]
  });

  it(
    'should be created',
    inject([PerformanceCounterService], (service: PerformanceCounterService) => {
      expect(service).toBeTruthy();
    })
  );
});
