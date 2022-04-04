import { TestBed } from '@angular/core/testing';

import { TextToDownloadService } from './text-to-download.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('TextToDownloadService', () => {
  let service: TextToDownloadService;

  configureTestBed({
    providers: [TextToDownloadService]
  });

  beforeEach(() => {
    service = TestBed.inject(TextToDownloadService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
