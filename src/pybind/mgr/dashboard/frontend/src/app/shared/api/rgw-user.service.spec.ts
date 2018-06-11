import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { RgwUserService } from './rgw-user.service';

describe('RgwUserService', () => {
  configureTestBed({
    providers: [RgwUserService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([RgwUserService], (service: RgwUserService) => {
      expect(service).toBeTruthy();
    })
  );
});
