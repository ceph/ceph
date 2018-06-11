import { HttpClientModule } from '@angular/common/http';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { CephfsService } from './cephfs.service';

describe('CephfsService', () => {
  configureTestBed({
    imports: [HttpClientModule],
    providers: [CephfsService]
  });

  it(
    'should be created',
    inject([CephfsService], (service: CephfsService) => {
      expect(service).toBeTruthy();
    })
  );
});
