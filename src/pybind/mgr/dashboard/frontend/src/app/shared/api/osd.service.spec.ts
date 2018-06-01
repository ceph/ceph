import { HttpClientModule } from '@angular/common/http';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { OsdService } from './osd.service';

describe('OsdService', () => {
  configureTestBed({
    providers: [OsdService],
    imports: [HttpClientModule]
  });

  it(
    'should be created',
    inject([OsdService], (service: OsdService) => {
      expect(service).toBeTruthy();
    })
  );
});
