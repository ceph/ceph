import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { appendFile } from 'fs';

import { DashboardService } from './dashboard.service';

describe('DashboardService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [DashboardService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it(
    'should be created',
    inject([DashboardService], (service: DashboardService) => {
      expect(service).toBeTruthy();
    })
  );
});
