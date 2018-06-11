import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { MonitorService } from './monitor.service';

describe('MonitorService', () => {
  configureTestBed({
    providers: [MonitorService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([MonitorService], (service: MonitorService) => {
      expect(service).toBeTruthy();
    })
  );
});
