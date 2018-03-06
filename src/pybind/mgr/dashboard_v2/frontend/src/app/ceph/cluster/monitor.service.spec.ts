import { HttpClientModule } from '@angular/common/http';
import {
  HttpClientTestingModule,
  HttpTestingController
} from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { MonitorService } from './monitor.service';

describe('MonitorService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MonitorService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it('should be created', inject([MonitorService], (service: MonitorService) => {
    expect(service).toBeTruthy();
  }));
});
