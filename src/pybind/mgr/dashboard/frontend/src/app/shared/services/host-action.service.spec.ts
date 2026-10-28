import { TestBed } from '@angular/core/testing';
import { TemplateRef } from '@angular/core';
import { of, throwError } from 'rxjs';

import { HostActionService } from './host-action.service';
import { HostService } from '~/app/shared/api/host.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Host } from '~/app/shared/models/host.interface';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('HostActionService', () => {
  let service: HostActionService;
  let hostService: HostService;
  let notificationService: NotificationService;
  let cdsModalService: ModalCdsService;
  let taskWrapper: TaskWrapperService;

  const mockHost: Host = {
    hostname: 'test-host',
    labels: ['mon', 'mgr'],
    status: HostStatus.AVAILABLE
  } as Host;

  const mockLabels = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'nfs', 'iscsi', 'rbd', 'grafana'];

  configureTestBed({
    providers: [
      HostActionService,
      {
        provide: HostService,
        useValue: {
          getLabels: jasmine.createSpy('getLabels').and.returnValue(of(mockLabels)),
          update: jasmine.createSpy('update').and.returnValue(of({})),
          delete: jasmine.createSpy('delete').and.returnValue(of({}))
        }
      },
      {
        provide: NotificationService,
        useValue: {
          show: jasmine.createSpy('show')
        }
      },
      {
        provide: ModalCdsService,
        useValue: {
          show: jasmine.createSpy('show').and.returnValue({ dismiss: () => {} }),
          dismissAll: jasmine.createSpy('dismissAll')
        }
      },
      {
        provide: TaskWrapperService,
        useValue: {
          wrapTaskAroundCall: jasmine.createSpy('wrapTaskAroundCall').and.returnValue(of({}))
        }
      }
    ]
  });

  beforeEach(() => {
    service = TestBed.inject(HostActionService);
    hostService = TestBed.inject(HostService);
    notificationService = TestBed.inject(NotificationService);
    cdsModalService = TestBed.inject(ModalCdsService);
    taskWrapper = TestBed.inject(TaskWrapperService);
  });

  afterEach(() => {
    (hostService.getLabels as jasmine.Spy).calls.reset();
    (hostService.update as jasmine.Spy).calls.reset();
    (hostService.delete as jasmine.Spy).calls.reset();
    (notificationService.show as jasmine.Spy).calls.reset();
    (cdsModalService.show as jasmine.Spy).calls.reset();
    (cdsModalService.dismissAll as jasmine.Spy).calls.reset();
    (taskWrapper.wrapTaskAroundCall as jasmine.Spy).calls.reset();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('openEditModal', () => {
    it('should open edit modal and call onSuccess with updated labels', (done) => {
      const updatedLabels = ['mon', 'mgr', 'new-label'];
      const onSuccess = jasmine.createSpy('onSuccess');

      service.openEditModal(mockHost, onSuccess);

      setTimeout(() => {
        expect(hostService.getLabels).toHaveBeenCalled();
        expect(cdsModalService.show).toHaveBeenCalled();

        // Simulate form submission
        const showCall = (cdsModalService.show as jasmine.Spy).calls.mostRecent();
        const modalConfig = showCall.args[1];
        modalConfig.onSubmit({ labels: updatedLabels });

        setTimeout(() => {
          expect(hostService.update).toHaveBeenCalledWith(mockHost.hostname, true, updatedLabels);
          expect(onSuccess).toHaveBeenCalledWith(updatedLabels);
          expect(notificationService.show).toHaveBeenCalledWith(
            NotificationType.success,
            `Updated Host "${mockHost.hostname}"`
          );
          done();
        }, 100);
      }, 100);
    });

    it('should handle empty labels', (done) => {
      const onSuccess = jasmine.createSpy('onSuccess');
      (hostService.getLabels as jasmine.Spy).and.returnValue(of([]));

      service.openEditModal(mockHost, onSuccess);

      setTimeout(() => {
        expect(cdsModalService.show).toHaveBeenCalled();
        done();
      }, 100);
    });
  });

  describe('hostMaintenance', () => {
    it('should enter maintenance mode for available host', (done) => {
      const maintenanceTpl = {} as TemplateRef<any>;
      const onExecutingChange = jasmine.createSpy('onExecutingChange');
      const onErrorMessage = jasmine.createSpy('onErrorMessage');
      const onSuccess = jasmine.createSpy('onSuccess');

      service.hostMaintenance(
        mockHost,
        maintenanceTpl,
        onExecutingChange,
        onErrorMessage,
        onSuccess
      );

      setTimeout(() => {
        expect(onExecutingChange).toHaveBeenCalledWith(true);
        expect(hostService.update).toHaveBeenCalledWith(mockHost.hostname, false, [], true);

        // Simulate successful update
        setTimeout(() => {
          expect(onExecutingChange).toHaveBeenCalledWith(false);
          expect(notificationService.show).toHaveBeenCalledWith(
            NotificationType.success,
            `"${mockHost.hostname}" moved to maintenance`
          );
          expect(onSuccess).toHaveBeenCalled();
          done();
        }, 100);
      }, 100);
    });

    it('should exit maintenance mode for host in maintenance', (done) => {
      const maintenanceHost = { ...mockHost, status: HostStatus.MAINTENANCE.toLowerCase() };
      const maintenanceTpl = {} as TemplateRef<any>;
      const onExecutingChange = jasmine.createSpy('onExecutingChange');
      const onErrorMessage = jasmine.createSpy('onErrorMessage');
      const onSuccess = jasmine.createSpy('onSuccess');

      service.hostMaintenance(
        maintenanceHost,
        maintenanceTpl,
        onExecutingChange,
        onErrorMessage,
        onSuccess
      );

      setTimeout(() => {
        expect(onExecutingChange).toHaveBeenCalledWith(true);
        expect(hostService.update).toHaveBeenCalledWith(maintenanceHost.hostname, false, [], true);

        setTimeout(() => {
          expect(notificationService.show).toHaveBeenCalledWith(
            NotificationType.success,
            `"${maintenanceHost.hostname}" has exited maintenance`
          );
          expect(onSuccess).toHaveBeenCalled();
          done();
        }, 100);
      }, 100);
    });

    it('should handle maintenance entry critical error', (done) => {
      const maintenanceTpl = {} as TemplateRef<any>;
      const onExecutingChange = jasmine.createSpy('onExecutingChange');
      const onErrorMessage = jasmine.createSpy('onErrorMessage');
      const onSuccess = jasmine.createSpy('onSuccess');

      const error = {
        error: { detail: 'ALERT: Cannot enter maintenance' },
        preventDefault: jasmine.createSpy('preventDefault')
      };

      (hostService.update as jasmine.Spy).and.returnValue(throwError(() => error));

      service.hostMaintenance(
        mockHost,
        maintenanceTpl,
        onExecutingChange,
        onErrorMessage,
        onSuccess
      );

      setTimeout(() => {
        setTimeout(() => {
          expect(notificationService.show).toHaveBeenCalledWith(
            NotificationType.error,
            `"${mockHost.hostname}" cannot be put into maintenance`,
            jasmine.any(String)
          );
          done();
        }, 100);
      }, 100);
    });
  });

  describe('deleteAction', () => {
    it('should open delete confirmation modal', () => {
      const hostname = 'test-host';

      service.deleteAction(hostname);

      expect(cdsModalService.show).toHaveBeenCalled();
      const showCall = (cdsModalService.show as jasmine.Spy).calls.mostRecent();
      const config = showCall.args[1];

      expect(config.impact).toBeDefined();
      expect(config.itemDescription).toBe('Host');
      expect(config.itemNames).toContain(hostname);
      expect(config.actionDescription).toBe('remove');
    });

    it('should call task wrapper on delete action confirmation', () => {
      const hostname = 'test-host';

      service.deleteAction(hostname);

      const showCall = (cdsModalService.show as jasmine.Spy).calls.mostRecent();
      const config = showCall.args[1];
      config.submitActionObservable();

      expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalled();
    });
  });

  describe('private helper methods', () => {
    it('should correctly get host labels', () => {
      const hostWithLabels = { ...mockHost, labels: ['mon', 'mgr'] };
      const result = (service as any).getHostLabels(hostWithLabels);

      expect(result).toEqual(['mon', 'mgr']);
      expect(result).not.toBe(hostWithLabels.labels); // Should be a clone
    });

    it('should handle undefined labels when getting host labels', () => {
      const hostNoLabels = { ...mockHost, labels: undefined };
      const result = (service as any).getHostLabels(hostNoLabels);

      expect(result).toEqual([]);
    });

    it('should create host labels field with correct structure', () => {
      const hostLabels = ['mon'];
      const allLabels = [
        { content: 'mon', selected: true },
        { content: 'mgr', selected: false }
      ];

      const result = (service as any).createHostLabelsField(hostLabels, allLabels);

      expect(result.type).toBe('select-badges');
      expect(result.name).toBe('labels');
      expect(result.value).toEqual(hostLabels);
      expect(result.typeConfig.customBadges).toBe(true);
    });

    it('should detect host in maintenance mode', () => {
      const availableHost = { ...mockHost, status: HostStatus.AVAILABLE };
      const maintenanceHost = { ...mockHost, status: HostStatus.MAINTENANCE.toLowerCase() };

      expect((service as any).isHostInMaintenance(availableHost)).toBe(false);
      expect((service as any).isHostInMaintenance(maintenanceHost)).toBe(true);
    });

    it('should determine if maintenance warning should show', () => {
      const warningMsg = 'WARNING: Some services will be stopped';
      const unsafeMsg = 'WARNING: It is NOT safe to stop';
      const alertMsg = 'ALERT: Critical error';

      expect((service as any).shouldShowMaintenanceWarning(warningMsg)).toBe(true);
      expect((service as any).shouldShowMaintenanceWarning(unsafeMsg)).toBe(false);
      expect((service as any).shouldShowMaintenanceWarning(alertMsg)).toBe(false);
    });

    it('should extract maintenance error detail', () => {
      const error = { error: { detail: 'Test error message' } };
      const errorNoDetail = { error: {} };
      const errorNoError = {};

      expect((service as any).getHostMaintenanceErrorDetail(error)).toBe('Test error message');
      expect((service as any).getHostMaintenanceErrorDetail(errorNoDetail)).toBe('');
      expect((service as any).getHostMaintenanceErrorDetail(errorNoError)).toBe('');
    });
  });
});
