import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { FaviconService } from './favicon.service';

describe('FaviconService', () => {
  let service: FaviconService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [FaviconService]
  });

  beforeEach(() => {
    service = TestBed.inject(FaviconService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
