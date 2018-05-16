import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ConfigurationService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it(
    'should be created',
    inject([ConfigurationService], (service: ConfigurationService) => {
      expect(service).toBeTruthy();
    })
  );
});
