import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { RgwUserService } from './rgw-user.service';

describe('RgwUserService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RgwUserService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it(
    'should be created',
    inject([RgwUserService], (service: RgwUserService) => {
      expect(service).toBeTruthy();
    })
  );
});
