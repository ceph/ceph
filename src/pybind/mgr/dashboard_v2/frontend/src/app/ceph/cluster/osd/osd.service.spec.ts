import { HttpClientModule } from '@angular/common/http';
import { inject, TestBed } from '@angular/core/testing';

import { OsdService } from './osd.service';

describe('OsdService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OsdService],
      imports: [
        HttpClientModule,
      ],
    });
  });

  it('should be created', inject([OsdService], (service: OsdService) => {
    expect(service).toBeTruthy();
  }));
});
