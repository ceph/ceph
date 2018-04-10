import { inject, TestBed } from '@angular/core/testing';

import { ToastOptions, ToastsManager } from 'ng2-toastr';

import { NotificationService } from './notification.service';

describe('NotificationService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [NotificationService, ToastsManager, ToastOptions]
    });
  });

  it('should be created', inject([NotificationService], (service: NotificationService) => {
    expect(service).toBeTruthy();
  }));
});
