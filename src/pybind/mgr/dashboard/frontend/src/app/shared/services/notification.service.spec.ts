import { DatePipe } from '@angular/common';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { ToastsManager } from 'ng2-toastr';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { NotificationService } from './notification.service';
import { TaskMessageService } from './task-message.service';

describe('NotificationService', () => {
  let service: NotificationService;
  const toastFakeService = {
    error: () => true,
    info: () => true,
    success: () => true
  };

  configureTestBed({
    providers: [
      CdDatePipe,
      DatePipe,
      NotificationService,
      TaskMessageService,
      { provide: ToastsManager, useValue: toastFakeService },
      { provide: CdDatePipe, useValue: { transform: (d) => d } },
      i18nProviders
    ]
  });

  beforeEach(() => {
    service = TestBed.get(NotificationService);
    service.removeAll();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should read empty notification list', () => {
    localStorage.setItem('cdNotifications', '[]');
    expect(service['dataSource'].getValue()).toEqual([]);
  });

  it('should read old notifications', fakeAsync(() => {
    localStorage.setItem(
      'cdNotifications',
      '[{"type":2,"message":"foobar","timestamp":"2018-05-24T09:41:32.726Z"}]'
    );
    service = new NotificationService(null, null, null);
    expect(service['dataSource'].getValue().length).toBe(1);
  }));

  it('should cancel a notification', fakeAsync(() => {
    const timeoutId = service.show(NotificationType.error, 'Simple test');
    service.cancel(timeoutId);
    tick(5000);
    expect(service['dataSource'].getValue().length).toBe(0);
  }));

  it('should create a success notification and save it', fakeAsync(() => {
    service.show(new CdNotificationConfig(NotificationType.success, 'Simple test'));
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(1);
    expect(service['dataSource'].getValue()[0].type).toBe(NotificationType.success);
  }));

  it('should create an error notification and save it', fakeAsync(() => {
    service.show(NotificationType.error, 'Simple test');
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(1);
    expect(service['dataSource'].getValue()[0].type).toBe(NotificationType.error);
  }));

  it('should create an info notification and save it', fakeAsync(() => {
    service.show(new CdNotificationConfig(NotificationType.info, 'Simple test'));
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(1);
    const notification = service['dataSource'].getValue()[0];
    expect(notification.type).toBe(NotificationType.info);
    expect(notification.title).toBe('Simple test');
    expect(notification.message).toBe(undefined);
  }));

  it('should never have more then 10 notifications', fakeAsync(() => {
    for (let index = 0; index < 15; index++) {
      service.show(NotificationType.info, 'Simple test');
      tick(100);
    }
    expect(service['dataSource'].getValue().length).toBe(10);
  }));

  it('should show a success task notification', fakeAsync(() => {
    const task = _.assign(new FinishedTask(), {
      success: true
    });
    service.notifyTask(task, true);
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(1);
    const notification = service['dataSource'].getValue()[0];
    expect(notification.type).toBe(NotificationType.success);
    expect(notification.title).toBe('Executed unknown task');
    expect(notification.message).toBe(undefined);
  }));

  it('should be able to stop notifyTask from notifying', fakeAsync(() => {
    const task = _.assign(new FinishedTask(), {
      success: true
    });
    const timeoutId = service.notifyTask(task, true);
    service.cancel(timeoutId);
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(0);
  }));

  it('should show a error task notification', fakeAsync(() => {
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
    service.notifyTask(task);
    tick(100);
    expect(service['dataSource'].getValue().length).toBe(1);
    const notification = service['dataSource'].getValue()[0];
    expect(notification.type).toBe(NotificationType.error);
    expect(notification.title).toBe(`Failed to create RBD 'somePool/someImage'`);
    expect(notification.message).toBe(`Name is already used by RBD 'somePool/someImage'.`);
  }));

  describe('notification queue', () => {
    const n1 = new CdNotificationConfig(NotificationType.success, 'Some success');
    const n2 = new CdNotificationConfig(NotificationType.info, 'Some info');

    beforeEach(() => {
      spyOn(service, 'show').and.stub();
    });

    it('filters out duplicated notifications on single call', fakeAsync(() => {
      service.queueNotifications([n1, n1, n2, n2]);
      tick(500);
      expect(service.show).toHaveBeenCalledTimes(2);
    }));

    it('filters out duplicated notifications presented in different calls', fakeAsync(() => {
      service.queueNotifications([n1, n2]);
      service.queueNotifications([n1, n2]);
      tick(500);
      expect(service.show).toHaveBeenCalledTimes(2);
    }));

    it('will reset the timeout on every call', fakeAsync(() => {
      service.queueNotifications([n1, n2]);
      tick(400);
      service.queueNotifications([n1, n2]);
      tick(100);
      expect(service.show).toHaveBeenCalledTimes(0);
      tick(400);
      expect(service.show).toHaveBeenCalledTimes(2);
    }));

    it('wont filter out duplicated notifications if timeout was reached before', fakeAsync(() => {
      service.queueNotifications([n1, n2]);
      tick(500);
      service.queueNotifications([n1, n2]);
      tick(500);
      expect(service.show).toHaveBeenCalledTimes(4);
    }));
  });

  describe('showToasty', () => {
    let toastr: ToastsManager;
    const time = '2022-02-22T00:00:00.000Z';

    beforeEach(() => {
      const baseTime = new Date(time);
      spyOn(global, 'Date').and.returnValue(baseTime);
      spyOn(window, 'setTimeout').and.callFake((fn) => fn());

      toastr = TestBed.get(ToastsManager);
      // spyOn needs to know the methods before spying and can't read the array for clarification
      ['error', 'info', 'success'].forEach((method: 'error' | 'info' | 'success') =>
        spyOn(toastr, method).and.stub()
      );
    });

    it('should show with only title defined', () => {
      service.show(NotificationType.info, 'Some info');
      expect(toastr.info).toHaveBeenCalledWith(
        `<small class="date">${time}</small>` +
          '<i class="pull-right custom-icon ceph-icon" title="Ceph"></i>',
        'Some info',
        undefined
      );
    });

    it('should show with title and message defined', () => {
      service.show(
        () =>
          new CdNotificationConfig(NotificationType.error, 'Some error', 'Some operation failed')
      );
      expect(toastr.error).toHaveBeenCalledWith(
        'Some operation failed<br>' +
          `<small class="date">${time}</small>` +
          '<i class="pull-right custom-icon ceph-icon" title="Ceph"></i>',
        'Some error',
        undefined
      );
    });

    it('should show with title, message and application defined', () => {
      service.show(
        new CdNotificationConfig(
          NotificationType.success,
          'Alert resolved',
          'Some alert resolved',
          undefined,
          'Prometheus'
        )
      );
      expect(toastr.success).toHaveBeenCalledWith(
        'Some alert resolved<br>' +
          `<small class="date">${time}</small>` +
          '<i class="pull-right custom-icon prometheus-icon" title="Prometheus"></i>',
        'Alert resolved',
        undefined
      );
    });
  });
});
