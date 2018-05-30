import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { ToastsManager } from 'ng2-toastr';

import { NotificationType } from '../enum/notification-type.enum';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { TaskManagerMessageService } from './task-manager-message.service';

describe('NotificationService', () => {
  let notificationService: NotificationService;
  const fakeService = {
    // ToastsManager
    error: () => true,
    info: () => true,
    success: () => true,
    // TaskManagerMessageService
    getDescription: () => true,
    getErrorMessage: () => true,
    getSuccessMessage: () => true
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        NotificationService,
        { provide: TaskManagerMessageService, useValue: fakeService },
        { provide: ToastsManager, useValue: fakeService }
      ]
    });

    notificationService = TestBed.get(NotificationService);
    notificationService.removeAll();
  });

  it('should be created', () => {
    expect(notificationService).toBeTruthy();
  });

  it('should read empty notification list', () => {
    localStorage.setItem('cdNotifications', '[]');
    expect(notificationService['dataSource'].getValue()).toEqual([]);
  });

  it(
    'should read old notifications',
    fakeAsync(() => {
      localStorage.setItem(
        'cdNotifications',
        '[{"type":2,"message":"foobar","timestamp":"2018-05-24T09:41:32.726Z"}]'
      );
      const service = new NotificationService(null, null);
      expect(service['dataSource'].getValue().length).toBe(1);
    })
  );

  it(
    'should cancel a notification',
    fakeAsync(() => {
      const timeoutId = notificationService.show(NotificationType.error, 'Simple test');
      notificationService.cancel(timeoutId);
      tick(5000);
      expect(notificationService['dataSource'].getValue().length).toBe(0);
    })
  );

  it(
    'should create a success notification and save it',
    fakeAsync(() => {
      notificationService.show(NotificationType.success, 'Simple test');
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      expect(notificationService['dataSource'].getValue()[0].type).toBe(NotificationType.success);
    })
  );

  it(
    'should create an error notification and save it',
    fakeAsync(() => {
      notificationService.show(NotificationType.error, 'Simple test');
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      expect(notificationService['dataSource'].getValue()[0].type).toBe(NotificationType.error);
    })
  );

  it(
    'should create an info notification and save it',
    fakeAsync(() => {
      notificationService.show(NotificationType.info, 'Simple test');
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      expect(notificationService['dataSource'].getValue()[0].type).toBe(NotificationType.info);
    })
  );

  it(
    'should never have more then 10 notifications',
    fakeAsync(() => {
      for (let index = 0; index < 15; index++) {
        notificationService.show(NotificationType.info, 'Simple test');
        tick(100);
      }
      expect(notificationService['dataSource'].getValue().length).toBe(10);
    })
  );

  it(
    'should show a success task notification',
    fakeAsync(() => {
      const task = _.assign(new FinishedTask(), {
        success: true
      });
      notificationService.notifyTask(task, true);
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      expect(notificationService['dataSource'].getValue()[0].type).toBe(NotificationType.success);
    })
  );

  it(
    'should show a error task notification',
    fakeAsync(() => {
      const task = _.assign(new FinishedTask(), {
        success: false
      });
      notificationService.notifyTask(task);
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      expect(notificationService['dataSource'].getValue()[0].type).toBe(NotificationType.error);
    })
  );
});
