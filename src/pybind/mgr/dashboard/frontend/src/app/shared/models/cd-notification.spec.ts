import { NotificationType } from '../enum/notification-type.enum';
import { CdNotification, CdNotificationConfig } from './cd-notification';

describe('cd-notification classes', () => {
  const expectObject = (something: any, expected: any) => {
    (Object.keys(expected) as (keyof typeof expected)[]).forEach((key) =>
      expect(something[key]).toEqual(expected[key])
    );
  };

  // As these Models have a view methods they need to be tested
  describe('CdNotificationConfig', () => {
    it('should create a new config without any parameters', () => {
      expectObject(new CdNotificationConfig(), {
        application: 'Ceph',
        applicationClass: 'ceph-icon',
        message: undefined,
        options: {
          lowContrast: true,
          type: undefined,
          title: '',
          subtitle: '',
          caption: ''
        },
        title: undefined,
        type: 1
      });
    });

    it('should create a new config with parameters', () => {
      expectObject(
        new CdNotificationConfig(
          NotificationType.error,
          'Some Alert',
          'Something failed',
          undefined,
          'Prometheus'
        ),
        {
          application: 'Prometheus',
          applicationClass: 'prometheus-icon',
          message: 'Something failed',
          options: {
            lowContrast: true,
            type: undefined,
            title: '',
            subtitle: '',
            caption: ''
          },
          title: 'Some Alert',
          type: 0
        }
      );
    });
  });

  describe('CdNotification', () => {
    beforeEach(() => {
      const baseTime = new Date('2022-02-22');
      spyOn(global, 'Date').and.returnValue(baseTime);
    });

    it('should create a new config without any parameters', () => {
      expectObject(new CdNotification(), {
        application: 'Ceph',
        applicationClass: 'ceph-icon',
        iconClass: 'information',
        message: undefined,
        options: {
          lowContrast: true,
          type: undefined,
          title: '',
          subtitle: '',
          caption: ''
        },
        textClass: 'text-info',
        timestamp: '2022-02-22T00:00:00.000Z',
        title: undefined,
        type: 1
      });
    });

    it('should create a new config with parameters', () => {
      expectObject(
        new CdNotification(
          new CdNotificationConfig(
            NotificationType.error,
            'Some Alert',
            'Something failed',
            undefined,
            'Prometheus'
          )
        ),
        {
          application: 'Prometheus',
          applicationClass: 'prometheus-icon',
          iconClass: 'warning--alt--filled',
          message: 'Something failed',
          options: {
            lowContrast: true,
            type: undefined,
            title: '',
            subtitle: '',
            caption: ''
          },
          textClass: 'text-danger',
          timestamp: '2022-02-22T00:00:00.000Z',
          title: 'Some Alert',
          type: 0
        }
      );
    });

    it('should expect the right success classes', () => {
      expectObject(new CdNotification(new CdNotificationConfig(NotificationType.success)), {
        iconClass: 'checkmark',
        textClass: 'text-success'
      });
    });
  });
});
