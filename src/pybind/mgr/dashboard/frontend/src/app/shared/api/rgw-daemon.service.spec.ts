import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { RgwDaemonService } from './rgw-daemon.service';

describe('RgwDaemonService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RgwDaemonService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it(
    'should be created',
    inject([RgwDaemonService], (service: RgwDaemonService) => {
      expect(service).toBeTruthy();
    })
  );
});
