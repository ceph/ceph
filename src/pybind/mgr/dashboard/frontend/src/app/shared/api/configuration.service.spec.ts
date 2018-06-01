import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  configureTestBed({
    providers: [ConfigurationService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([ConfigurationService], (service: ConfigurationService) => {
      expect(service).toBeTruthy();
    })
  );
});
