import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { ToastsManager } from 'ng2-toastr';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { NotificationType } from '../enum/notification-type.enum';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { TaskMessageService } from './task-message.service';

describe('NotificationService', () => {
  let notificationService: NotificationService;
  const toastFakeService = {
    error: () => true,
    info: () => true,
    success: () => true
  };

  configureTestBed({
    providers: [
      NotificationService,
      TaskMessageService,
      { provide: ToastsManager, useValue: toastFakeService }
    ]
  });

  beforeEach(() => {
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
      const notification = notificationService['dataSource'].getValue()[0];
      expect(notification.type).toBe(NotificationType.info);
      expect(notification.title).toBe('Simple test');
      expect(notification.message).toBe(undefined);
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
      const notification = notificationService['dataSource'].getValue()[0];
      expect(notification.type).toBe(NotificationType.success);
      expect(notification.title).toBe('Executed unknown task');
      expect(notification.message).toBe(undefined);
    })
  );

  it(
    'should show a error task notification',
    fakeAsync(() => {
      const task = _.assign(
        new FinishedTask('rbd/create', {
          pool_name: 'somePool',
          image_name: 'someImage'
        }),
        {
          success: false,
          exception: {
            code: 17
          }
        }
      );
      notificationService.notifyTask(task);
      tick(100);
      expect(notificationService['dataSource'].getValue().length).toBe(1);
      const notification = notificationService['dataSource'].getValue()[0];
      expect(notification.type).toBe(NotificationType.error);
      expect(notification.title).toBe(`Failed to create RBD 'somePool/someImage'`);
      expect(notification.message).toBe(`Name is already used by RBD 'somePool/someImage'.`);
    })
  );
});
