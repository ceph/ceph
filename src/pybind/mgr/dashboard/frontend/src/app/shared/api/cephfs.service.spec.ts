import { HttpClientModule } from '@angular/common/http';
import { inject, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { CephfsService } from './cephfs.service';

describe('CephfsService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule],
      providers: [CephfsService]
    });
  });

  it(
    'should be created',
    inject([CephfsService], (service: CephfsService) => {
      expect(service).toBeTruthy();
    })
  );
});
