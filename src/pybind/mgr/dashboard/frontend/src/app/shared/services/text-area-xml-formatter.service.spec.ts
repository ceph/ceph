import { TestBed } from '@angular/core/testing';

import { TextAreaXmlFormatterService } from './text-area-xml-formatter.service';

describe('TextAreaXmlFormatterService', () => {
  let service: TextAreaXmlFormatterService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TextAreaXmlFormatterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
