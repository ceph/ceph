import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { ToastrService } from 'ngx-toastr';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { RbdService } from '../api/rbd.service';
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
      NotificationService,
      TaskMessageService,
      { provide: ToastrService, useValue: toastFakeService },
      { provide: CdDatePipe, useValue: { transform: (d: any) => d } },
      i18nProviders,
      RbdService
    ],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
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
    tick(5000);
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
      addNotifications(15);
      expect(service['dataSource'].getValue().length).toBe(10);
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

      tick(1500);

      expect(service.show).toHaveBeenCalled();
      const notifications = service['dataSource'].getValue();
      expect(notifications.length).toBe(0);
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

    it('should remove a single notification', fakeAsync(() => {
      addNotifications(5);
      let messages = service['dataSource'].getValue().map((notification) => notification.title);
      expect(messages).toEqual(['4', '3', '2', '1', '0']);
      service.remove(2);
      messages = service['dataSource'].getValue().map((notification) => notification.title);
      expect(messages).toEqual(['4', '3', '1', '0']);
    }));

    it('should remove all notifications', fakeAsync(() => {
      addNotifications(5);
      expect(service['dataSource'].getValue().length).toBe(5);
      service.removeAll();
      expect(service['dataSource'].getValue().length).toBe(0);
    }));
  });

  describe('notification queue', () => {
    const n1 = new CdNotificationConfig(NotificationType.success, 'Some success');
    const n2 = new CdNotificationConfig(NotificationType.info, 'Some info');

    const showArray = (arr: any[]) => arr.forEach((n) => service.show(n));

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

      toastr = TestBed.inject(ToastrService);
      // spyOn needs to know the methods before spying and can't read the array for clarification
      ['error', 'info', 'success'].forEach((method: 'error' | 'info' | 'success') =>
        spyOn(toastr, method).and.stub()
      );
    });

    it('should show with only title defined', () => {
      service.show(NotificationType.info, 'Some info');
      expect(toastr.info).toHaveBeenCalledWith(
        `<small class="date">${time}</small>` +
          '<i class="float-right custom-icon ceph-icon" title="Ceph"></i>',
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
          '<i class="float-right custom-icon ceph-icon" title="Ceph"></i>',
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
          '<i class="float-right custom-icon prometheus-icon" title="Prometheus"></i>',
        'Alert resolved',
        undefined
      );
    });
  });
});
