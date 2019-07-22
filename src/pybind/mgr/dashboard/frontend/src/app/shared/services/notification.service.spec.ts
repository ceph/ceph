import { DatePipe } from '@angular/common';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { ToastrService } from 'ngx-toastr';

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
      { provide: ToastrService, useValue: toastFakeService },
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

  describe('Saved notifications', () => {
    const expectSavedNotificationToHave = (expected: {}) => {
      tick(510);
      expect(service['dataSource'].getValue().length).toBe(1);
      const notification = service['dataSource'].getValue()[0];
      Object.keys(expected).forEach((key) => {
        expect(notification[key]).toBe(expected[key]);
      });
    };

    beforeEach(() => {
      service.cancel(service['justShownTimeoutId']);
    });

    it('should create a success notification and save it', fakeAsync(() => {
      service.show(new CdNotificationConfig(NotificationType.success, 'Simple test'));
      expectSavedNotificationToHave({ type: NotificationType.success });
    }));

    it('should create an error notification and save it', fakeAsync(() => {
      service.show(NotificationType.error, 'Simple test');
      expectSavedNotificationToHave({ type: NotificationType.error });
    }));

    it('should create an info notification and save it', fakeAsync(() => {
      service.show(new CdNotificationConfig(NotificationType.info, 'Simple test'));
      expectSavedNotificationToHave({
        type: NotificationType.info,
        title: 'Simple test',
        message: undefined
      });
    }));

    it('should never have more then 10 notifications', fakeAsync(() => {
      for (let index = 0; index < 15; index++) {
        service.show(NotificationType.info, 'Simple test');
        tick(510);
      }
      expect(service['dataSource'].getValue().length).toBe(10);
    }));

    it('should show a success task notification', fakeAsync(() => {
      const task = _.assign(new FinishedTask(), {
        success: true
      });
      service.notifyTask(task, true);
      expectSavedNotificationToHave({
        type: NotificationType.success,
        title: 'Executed unknown task',
        message: undefined
      });
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
      expectSavedNotificationToHave({
        type: NotificationType.error,
        title: `Failed to create RBD 'somePool/someImage'`,
        message: `Name is already used by RBD 'somePool/someImage'.`
      });
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
    }));
  });

  describe('notification queue', () => {
    const n1 = new CdNotificationConfig(NotificationType.success, 'Some success');
    const n2 = new CdNotificationConfig(NotificationType.info, 'Some info');

    const showArray = (arr) => arr.forEach((n) => service.show(n));

    beforeEach(() => {
      spyOn(service, 'save').and.stub();
    });

    it('filters out duplicated notifications on single call', fakeAsync(() => {
      showArray([n1, n1, n2, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(2);
    }));

    it('filters out duplicated notifications presented in different calls', fakeAsync(() => {
      showArray([n1, n2]);
      showArray([n1, n2]);
      tick(1000);
      expect(service.save).toHaveBeenCalledTimes(2);
    }));

    it('will reset the timeout on every call', fakeAsync(() => {
      showArray([n1, n2]);
      tick(490);
      showArray([n1, n2]);
      tick(450);
      expect(service.save).toHaveBeenCalledTimes(0);
      tick(60);
      expect(service.save).toHaveBeenCalledTimes(2);
    }));

    it('wont filter out duplicated notifications if timeout was reached before', fakeAsync(() => {
      showArray([n1, n2]);
      tick(510);
      showArray([n1, n2]);
      tick(510);
      expect(service.save).toHaveBeenCalledTimes(4);
    }));
  });

  describe('showToasty', () => {
    let toastr: ToastrService;
    const time = '2022-02-22T00:00:00.000Z';

    beforeEach(() => {
      const baseTime = new Date(time);
      spyOn(global, 'Date').and.returnValue(baseTime);
      spyOn(window, 'setTimeout').and.callFake((fn) => fn());

      toastr = TestBed.get(ToastrService);
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
