import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { TextToDownloadService } from './text-to-download.service';

describe('TextToDownloadService', () => {
  let service: TextToDownloadService;

  configureTestBed({
    providers: [TextToDownloadService]
  });

  beforeEach(() => {
    service = TestBed.get(TextToDownloadService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
