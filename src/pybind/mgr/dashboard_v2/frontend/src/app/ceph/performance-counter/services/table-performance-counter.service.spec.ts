import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap';

import { TablePerformanceCounterService } from './table-performance-counter.service';

describe('TablePerformanceCounterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TablePerformanceCounterService],
      imports: [
        HttpClientTestingModule,
        BsDropdownModule.forRoot(),
        HttpClientModule
      ]
    });
  });

  it(
    'should be created',
    inject([TablePerformanceCounterService], (service: TablePerformanceCounterService) => {
      expect(service).toBeTruthy();
    })
  );
});
