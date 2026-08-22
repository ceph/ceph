import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { CephfsActionService } from './cephfs-action.service';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { HealthService } from '~/app/shared/api/health.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsMountDetailsComponent } from '~/app/ceph/cephfs/cephfs-mount-details/cephfs-mount-details.component';
import { CephfsAuthModalComponent } from '~/app/ceph/cephfs/cephfs-auth-modal/cephfs-auth-modal.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';
import { TemplateRef } from '@angular/core';

describe('CephfsActionService', () => {
  let service: CephfsActionService;
  let cephfsServiceMock: any;
  let configurationServiceMock: any;
  let healthServiceMock: any;
  let modalServiceMock: any;
  let taskWrapperServiceMock: any;
  let mockModalRef: any;

  beforeEach(() => {
    cephfsServiceMock = {
      getFsRootDirectory: jest.fn(),
      remove: jest.fn()
    };

    configurationServiceMock = {
      get: jest.fn()
    };

    healthServiceMock = {
      getClusterFsid: jest.fn()
    };

    mockModalRef = { close: jest.fn() };
    modalServiceMock = {
      show: jest.fn().mockReturnValue(mockModalRef)
    };

    taskWrapperServiceMock = {
      wrapTaskAroundCall: jest.fn()
    };

    TestBed.configureTestingModule({
      providers: [
        CephfsActionService,
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: ConfigurationService, useValue: configurationServiceMock },
        { provide: HealthService, useValue: healthServiceMock },
        { provide: ModalCdsService, useValue: modalServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperServiceMock }
      ]
    });

    service = TestBed.inject(CephfsActionService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('getMonAllowPoolDelete', () => {
    it('should return true if mon_allow_pool_delete is configured to true', (done) => {
      configurationServiceMock.get.mockReturnValue(
        of({ value: [{ section: 'mon', value: 'true' }] })
      );

      service.getMonAllowPoolDelete().subscribe((result) => {
        expect(result).toBe(true);
        done();
      });
    });

    it('should return false if mon_allow_pool_delete is configured to false', (done) => {
      configurationServiceMock.get.mockReturnValue(
        of({ value: [{ section: 'mon', value: 'false' }] })
      );

      service.getMonAllowPoolDelete().subscribe((result) => {
        expect(result).toBe(false);
        done();
      });
    });

    it('should return false if mon section is missing', (done) => {
      configurationServiceMock.get.mockReturnValue(
        of({ value: [{ section: 'mgr', value: 'true' }] })
      );

      service.getMonAllowPoolDelete().subscribe((result) => {
        expect(result).toBe(false);
        done();
      });
    });

    it('should catch errors and return false', (done) => {
      configurationServiceMock.get.mockReturnValue(throwError(() => new Error('API Error')));

      service.getMonAllowPoolDelete().subscribe((result) => {
        expect(result).toBe(false);
        done();
      });
    });
  });

  describe('getDeleteDisableDesc', () => {
    it('should return true if there is no selection', () => {
      expect(service.getDeleteDisableDesc(false, true)).toBe(true);
      expect(service.getDeleteDisableDesc(false, false)).toBe(true);
    });

    it('should return false if there is a selection and mon_allow_pool_delete is true', () => {
      expect(service.getDeleteDisableDesc(true, true)).toBe(false);
    });

    it('should return an error string if there is a selection but mon_allow_pool_delete is false', () => {
      const result = service.getDeleteDisableDesc(true, false);
      expect(typeof result).toBe('string');
      expect(result).toContain('mon_allow_pool_delete');
    });
  });

  describe('showAttachInfo', () => {
    it('should return early if selectedFileSystem has no id', () => {
      service.showAttachInfo({} as CephfsDetail);
      expect(cephfsServiceMock.getFsRootDirectory).not.toHaveBeenCalled();
    });

    it('should fetch directory and fsid, then open the mount details modal', () => {
      const selectedFileSystem = {
        id: 123,
        mdsmap: { fs_name: 'my-fs' }
      } as CephfsDetail;

      cephfsServiceMock.getFsRootDirectory.mockReturnValue(of({ path: '/cephfs' }));
      healthServiceMock.getClusterFsid.mockReturnValue(of('cluster-1234'));

      service.showAttachInfo(selectedFileSystem);

      expect(cephfsServiceMock.getFsRootDirectory).toHaveBeenCalledWith('123');
      expect(healthServiceMock.getClusterFsid).toHaveBeenCalled();

      expect(modalServiceMock.show).toHaveBeenCalledWith(CephfsMountDetailsComponent, {
        onSubmit: expect.any(Function),
        mountData: {
          clusterFSID: 'cluster-1234',
          fsName: 'my-fs',
          path: '/cephfs'
        }
      });
    });

    it('should trigger modal close when onSubmit is called from the modal', () => {
      const selectedFileSystem = { id: 123, mdsmap: { fs_name: 'my-fs' } } as CephfsDetail;

      cephfsServiceMock.getFsRootDirectory.mockReturnValue(of({ path: '/cephfs' }));
      healthServiceMock.getClusterFsid.mockReturnValue(of('cluster-1234'));

      service.showAttachInfo(selectedFileSystem);

      // Extract the onSubmit function passed to the modal
      const modalConfig = modalServiceMock.show.mock.calls[0][1];

      // Execute the onSubmit callback
      modalConfig.onSubmit();

      // Verify that it closed the modal reference
      expect(mockModalRef.close).toHaveBeenCalled();
    });
  });

  describe('removeVolume', () => {
    it('should return early if volName is empty', () => {
      service.removeVolume('');
      expect(modalServiceMock.show).not.toHaveBeenCalled();
    });

    it('should open the delete confirmation modal with correct configuration', () => {
      const mockTemplate = {} as TemplateRef<any>;

      service.removeVolume('my-vol', mockTemplate);

      expect(modalServiceMock.show).toHaveBeenCalledWith(DeleteConfirmationModalComponent, {
        impact: DeletionImpact.high,
        itemDescription: 'File System',
        itemNames: ['my-vol'],
        actionDescription: 'remove',
        bodyTemplate: mockTemplate,
        submitActionObservable: expect.any(Function)
      });
    });

    it('should execute cephfs remove through taskWrapper when submitActionObservable is called', () => {
      const cephfsRemoveObservable = of(null);
      cephfsServiceMock.remove.mockReturnValue(cephfsRemoveObservable);

      service.removeVolume('my-vol');

      const modalConfig = modalServiceMock.show.mock.calls[0][1];

      // Trigger the observable wrapper passed to the modal
      modalConfig.submitActionObservable();

      expect(cephfsServiceMock.remove).toHaveBeenCalledWith('my-vol');
      expect(taskWrapperServiceMock.wrapTaskAroundCall).toHaveBeenCalledWith({
        task: expect.objectContaining({
          name: 'cephfs/remove',
          metadata: { volumeName: 'my-vol' }
        }),
        call: cephfsRemoveObservable
      });
    });
  });

  describe('authorize', () => {
    it('should return early if selectedFileSystem is missing id or fs_name', () => {
      service.authorize(null);
      service.authorize({ id: 123 } as CephfsDetail); // missing fs_name
      service.authorize({ mdsmap: { fs_name: 'my-fs' } } as CephfsDetail); // missing id

      expect(modalServiceMock.show).not.toHaveBeenCalled();
    });

    it('should open the Auth Modal if selectedFileSystem is valid', () => {
      const selectedFileSystem = {
        id: 123,
        mdsmap: { fs_name: 'my-fs' }
      } as CephfsDetail;

      service.authorize(selectedFileSystem);

      expect(modalServiceMock.show).toHaveBeenCalledWith(CephfsAuthModalComponent, {
        fsName: 'my-fs',
        id: 123
      });
    });
  });
});
