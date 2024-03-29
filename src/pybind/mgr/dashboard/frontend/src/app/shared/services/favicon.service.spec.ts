import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { configureTestBed } from '~/testing/unit-test-helper';
import { FaviconService } from './favicon.service';

describe('FaviconService', () => {
  let service: FaviconService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [FaviconService, CssHelper]
  });

  beforeEach(() => {
    service = TestBed.inject(FaviconService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
