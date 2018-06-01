import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap';

import { configureTestBed } from '../unit-test-helper';
import { TablePerformanceCounterService } from './table-performance-counter.service';

describe('TablePerformanceCounterService', () => {
  configureTestBed({
    providers: [TablePerformanceCounterService],
    imports: [
      HttpClientTestingModule,
      BsDropdownModule.forRoot(),
      HttpClientModule
    ]
  });

  it(
    'should be created',
    inject([TablePerformanceCounterService], (service: TablePerformanceCounterService) => {
      expect(service).toBeTruthy();
    })
  );
});
