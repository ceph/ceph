import { HttpClientTestingModule } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick, flush } from '@angular/core/testing';
import _ from 'lodash';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdService } from '../api/rbd.service';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { NotificationService } from './notification.service';
import { TaskMessageService } from './task-message.service';

describe('NotificationService', () => {
  let service: NotificationService;

  configureTestBed({
    providers: [
      NotificationService,
      TaskMessageService,
      { provide: CdDatePipe, useValue: { transform: (d: any) => d } },
      RbdService
    ],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    spyOn(window, 'setTimeout').and.callFake((fn: Function) => {
      fn();
      return 0;
    });
    service = TestBed.inject(NotificationService);
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
    flush();
    expect(service['dataSource'].getValue().length).toBe(0);
  }));

  describe('Saved notifications', () => {
    const expectSavedNotificationToHave = (expected: object) => {
      tick(510);
      expect(service['dataSource'].getValue().length).toBe(1);
      const notification = service['dataSource'].getValue()[0];
      Object.keys(expected).forEach((key) => {
        expect(notification[key]).toBe(expected[key]);
      });
      flush();
    };

    const addNotifications = (quantity: number) => {
      for (let index = 0; index < quantity; index++) {
        service.show(NotificationType.info, `${index}`);
        tick(510);
      }
    };

    beforeEach(() => {
      spyOn(service, 'show').and.callThrough();
      service.cancel((<any>service)['justShownTimeoutId']);
    });

    it('should create a success notification and save it', fakeAsync(() => {
      service.show(new CdNotificationConfig(NotificationType.success, 'Simple test'));
      expectSavedNotificationToHave({ type: NotificationType.success });
      flush();
    }));

    it('should create an error notification and save it', fakeAsync(() => {
      service.show(NotificationType.error, 'Simple test');
      expectSavedNotificationToHave({ type: NotificationType.error });
      flush();
    }));

    it('should create an info notification and save it', fakeAsync(() => {
      service.show(new CdNotificationConfig(NotificationType.info, 'Simple test'));
      expectSavedNotificationToHave({
        type: NotificationType.info,
        title: 'Simple test',
        message: undefined
      });
      flush();
    }));

    it('should never have more then 10 notifications', fakeAsync(() => {
      addNotifications(15);
      expect(service['dataSource'].getValue().length).toBe(10);
      flush();
    }));

    it('should show a success task notification, but not save it', fakeAsync(() => {
      const task = _.assign(new FinishedTask(), {
        success: true
      });

      service.notifyTask(task, true);
      tick(1500);

      expect(service.show).toHaveBeenCalled();
      const notifications = service['dataSource'].getValue();
      expect(notifications.length).toBe(0);
      flush();
    }));

    it('should be able to stop notifyTask from notifying', fakeAsync(() => {
      const task = _.assign(new FinishedTask(), {
        success: true
      });
      const timeoutId = service.notifyTask(task, true);
      service.cancel(timeoutId);
      tick(100);
      expect(service['dataSource'].getValue().length).toBe(0);
      flush();
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

      tick(1500);

      expect(service.show).toHaveBeenCalled();
      const notifications = service['dataSource'].getValue();
      expect(notifications.length).toBe(0);
      flush();
    }));

    it('combines different notifications with the same title', fakeAsync(() => {
      service.show(NotificationType.error, '502 - Bad Gateway', 'Error occurred in path a');
      tick(60);
      service.show(NotificationType.error, '502 - Bad Gateway', 'Error occurred in path b');
      expectSavedNotificationToHave({
        type: NotificationType.error,
        title: '502 - Bad Gateway',
        message: '<ul><li>Error occurred in path a</li><li>Error occurred in path b</li></ul>'
      });
      flush();
    }));

    it('should remove a single notification', fakeAsync(() => {
      addNotifications(5);
      let messages = service['dataSource'].getValue().map((notification) => notification.title);
      expect(messages).toEqual(['4', '3', '2', '1', '0']);
      service.remove(2);
      messages = service['dataSource'].getValue().map((notification) => notification.title);
      expect(messages).toEqual(['4', '3', '1', '0']);
      flush();
    }));

    it('should remove all notifications', fakeAsync(() => {
      addNotifications(5);
      expect(service['dataSource'].getValue().length).toBe(5);
      service.removeAll();
      expect(service['dataSource'].getValue().length).toBe(0);
      flush();
    }));
  });

  describe('notification queue', () => {
    const n1 = new CdNotificationConfig(NotificationType.success, 'Some success');
    const n2 = new CdNotificationConfig(NotificationType.info, 'Some info');

    const showArray = (arr: any[]) => {
      arr.forEach((n) => service.show(n));
      tick(20);
    };

    beforeEach(() => {
      spyOn(service, 'save').and.stub();
    });

    it('filters out duplicated notifications on single call', fakeAsync(() => {
      showArray([n1, n1, n2, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(2);
      flush();
    }));

    it('filters out duplicated notifications presented in different calls', fakeAsync(() => {
      showArray([n1, n2]);
      tick(510);
      showArray([n1, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(4);
      flush();
    }));

    it('will reset the timeout on every call', fakeAsync(() => {
      showArray([n1, n2]);
      tick(400);
      showArray([n1, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(2);
      flush();
    }));

    it('wont filter out duplicated notifications if timeout was reached before', fakeAsync(() => {
      showArray([n1, n2]);
      tick(510);
      (service.save as jasmine.Spy).calls.reset();
      showArray([n1, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(2);
      flush();
    }));
  });

  describe('showToasty', () => {
    const time = '2022-02-22T00:00:00.000Z';

    beforeEach(() => {
      const baseTime = new Date(time);
      spyOn(global, 'Date').and.returnValue(baseTime);
    });

    it('should show with only title defined', fakeAsync(() => {
      service.show(NotificationType.info, 'Some info');
      tick(510);
      const toasts = service['activeToastsSource'].getValue();
      expect(toasts.length).toBe(1);
      expect(toasts[0].title).toBe('Some info');
      flush();
    }));

    it('should show with title and message defined', fakeAsync(() => {
      service.show(
        () =>
          new CdNotificationConfig(NotificationType.error, 'Some error', 'Some operation failed')
      );
      tick(510);
      const toasts = service['activeToastsSource'].getValue();
      expect(toasts.length).toBe(1);
      expect(toasts[0].title).toBe('Some error');
      expect(toasts[0].subtitle).toBe('Some operation failed');
      flush();
    }));

    it('should show with title, message and application defined (application name hidden)', fakeAsync(() => {
      service.show(
        new CdNotificationConfig(
          NotificationType.success,
          'Alert resolved',
          'Some alert resolved',
          undefined,
          'Prometheus'
        )
      );
      tick(510);
      const toasts = service['activeToastsSource'].getValue();
      expect(toasts.length).toBe(1);
      expect(toasts[0].title).toBe('Alert resolved');
      expect(toasts[0].subtitle).toBe('Some alert resolved');
      expect(toasts[0].caption).not.toContain('Prometheus');
      flush();
    }));
  });
});
