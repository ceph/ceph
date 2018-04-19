import { inject, TestBed } from '@angular/core/testing';

import { ToastOptions, ToastsManager } from 'ng2-toastr';

import { NotificationType } from '../enum/notification-type.enum';
import { NotificationService } from './notification.service';
import { TaskManagerMessageService } from './task-manager-message.service';
import { TaskManagerService } from './task-manager.service';

describe('NotificationService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        NotificationService,
        ToastsManager,
        ToastOptions,
        TaskManagerService,
        TaskManagerMessageService
      ]
    });
  });

  it('should be created',
    inject([NotificationService], (service: NotificationService) => {
      expect(service).toBeTruthy();
    }));

  it('should not create a notification',
    inject([NotificationService], (service: NotificationService) => {
      expect(service).toBeTruthy();
      service.removeAll();
      const timeoutId = service.show(NotificationType.error, 'Simple test');
      service.cancel(timeoutId);
      expect(service['dataSource'].getValue().length).toBe(0);
    }));
});
