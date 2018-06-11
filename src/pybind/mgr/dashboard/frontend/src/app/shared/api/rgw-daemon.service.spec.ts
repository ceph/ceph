import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { RgwDaemonService } from './rgw-daemon.service';

describe('RgwDaemonService', () => {
  configureTestBed({
    providers: [RgwDaemonService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([RgwDaemonService], (service: RgwDaemonService) => {
      expect(service).toBeTruthy();
    })
  );
});
