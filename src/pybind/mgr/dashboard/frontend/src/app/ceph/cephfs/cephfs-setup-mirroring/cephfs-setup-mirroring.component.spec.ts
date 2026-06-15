import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';

import { CephfsSetupMirroringComponent } from './cephfs-setup-mirroring.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';

describe('CephfsSetupMirroringComponent', () => {
  let component: CephfsSetupMirroringComponent;
  let fixture: ComponentFixture<CephfsSetupMirroringComponent>;

  const cephfsServiceMock = {
    list: jest.fn().mockReturnValue(of([])),
    listDaemonStatus: jest.fn().mockReturnValue(of([])),
    enableMirror: jest.fn().mockReturnValue(of(null)),
    createBootstrapPeer: jest.fn().mockReturnValue(of(null))
  };

  const cephServiceServiceMock = {
    create: jest.fn().mockReturnValue(of(null))
  };

  const taskWrapperMock = {
    wrapTaskAroundCall: jest.fn().mockImplementation(({ call }) => call)
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    cephfsServiceMock.list.mockReturnValue(of([]));
    cephfsServiceMock.listDaemonStatus.mockReturnValue(of([]));
    cephfsServiceMock.enableMirror.mockReturnValue(of(null));
    cephfsServiceMock.createBootstrapPeer.mockReturnValue(of(null));

    await TestBed.configureTestingModule({
      declarations: [CephfsSetupMirroringComponent],
      imports: [ReactiveFormsModule],
      providers: [
        CdFormBuilder,
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: CephServiceService, useValue: cephServiceServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsSetupMirroringComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsSetupMirroringComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('ngOnInit', () => {
    it('should fetch filesystems and populate the list', () => {
      cephfsServiceMock.list.mockReturnValue(
        of([
          { id: 1, mdsmap: { fs_name: 'myfs' } },
          { id: 2, mdsmap: { fs_name: 'backup' } }
        ])
      );
      fixture.detectChanges();

      expect(cephfsServiceMock.list).toHaveBeenCalled();
      expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalled();
      expect(component.filesystems).toEqual([
        { id: 1, name: 'myfs' },
        { id: 2, name: 'backup' }
      ]);
    });

    it('should filter out filesystems that already have mirror peers', () => {
      cephfsServiceMock.list.mockReturnValue(
        of([
          { id: 1, mdsmap: { fs_name: 'myfs' } },
          { id: 2, mdsmap: { fs_name: 'mirrored-fs' }, mirror_info: { peers: { uuid: {} } } },
          { id: 3, mdsmap: { fs_name: 'backup' } }
        ])
      );
      cephfsServiceMock.listDaemonStatus.mockReturnValue(
        of([
          {
            daemon_id: 1,
            filesystems: [
              {
                filesystem_id: 4,
                name: 'daemon-mirrored-fs',
                directory_count: 0,
                peers: [{ uuid: 'peer-1', remote: {}, stats: {} }],
                id: ''
              }
            ]
          }
        ])
      );
      fixture.detectChanges();

      expect(component.filesystems).toEqual([
        { id: 1, name: 'myfs' },
        { id: 3, name: 'backup' }
      ]);
    });

    it('should fall back to fs-<id> when mdsmap.fs_name is missing', () => {
      cephfsServiceMock.list.mockReturnValue(of([{ id: 5, mdsmap: {} }]));
      fixture.detectChanges();

      expect(component.filesystems).toEqual([{ id: 5, name: 'fs-5' }]);
    });
  });

  describe('form validation', () => {
    beforeEach(() => fixture.detectChanges());

    it('should be invalid when empty', () => {
      expect(component.setupForm.invalid).toBe(true);
    });

    it('should be valid when filesystem and token are filled', () => {
      component.setupForm.setValue({ filesystem: 'myfs', token: 'eyJrZXkiOiJ2YWx1ZSJ9' });
      expect(component.setupForm.valid).toBe(true);
    });

    it('should be invalid when filesystem is missing', () => {
      component.setupForm.setValue({ filesystem: '', token: 'eyJrZXkiOiJ2YWx1ZSJ9' });
      expect(component.setupForm.invalid).toBe(true);
    });

    it('should be invalid when token is missing', () => {
      component.setupForm.setValue({ filesystem: 'myfs', token: '' });
      expect(component.setupForm.invalid).toBe(true);
    });

    it('should be invalid when token is not valid base64 JSON', () => {
      component.setupForm.setValue({ filesystem: 'myfs', token: 'not-a-valid-token' });
      expect(component.setupForm.controls['token'].hasError('invalidBase64Json')).toBe(true);
      expect(component.setupForm.invalid).toBe(true);
    });

    it('should accept a valid bootstrap token with surrounding whitespace', () => {
      const token = btoa(JSON.stringify({ fsid: 'abc', key: 'value' }));
      component.setupForm.setValue({ filesystem: 'myfs', token: `  ${token}  ` });
      expect(component.setupForm.valid).toBe(true);
    });
  });

  describe('onSetupMirroring', () => {
    beforeEach(() => fixture.detectChanges());

    it('should not submit when form is invalid', () => {
      component.onSetupMirroring();
      expect(taskWrapperMock.wrapTaskAroundCall).not.toHaveBeenCalled();
    });

    it('should expose localized submit button text', () => {
      expect(component.submitText).toBe('Setup mirroring');

      component.isSubmitting = true;
      expect(component.submitText).toBe('Setting up...');
    });

    it('should deploy service, enable mirror, create peer, and emit on success', () => {
      const emitSpy = jest.spyOn(component.mirroringSetup, 'emit');
      component.setupForm.setValue({ filesystem: 'myfs', token: '  eyJrZXkiOiJ2YWx1ZSJ9  ' });

      component.onSetupMirroring();

      expect(cephServiceServiceMock.create).toHaveBeenCalledWith({ service_type: 'cephfs-mirror' });
      expect(cephfsServiceMock.enableMirror).toHaveBeenCalledWith('myfs');
      expect(cephfsServiceMock.createBootstrapPeer).toHaveBeenCalledWith(
        'myfs',
        'eyJrZXkiOiJ2YWx1ZSJ9'
      );
      expect(taskWrapperMock.wrapTaskAroundCall).toHaveBeenCalledWith(
        expect.objectContaining({
          task: expect.objectContaining({ name: 'cephfs/mirroring/setup' })
        })
      );
      expect(component.isSubmitting).toBe(false);
      expect(emitSpy).toHaveBeenCalledWith({ filesystem: 'myfs' });
    });

    it('should strip all whitespace from the token before creating the peer', () => {
      component.setupForm.setValue({
        filesystem: 'myfs',
        token: '  eyJr\nZXkiOiJ2YWx1ZSJ9  '
      });

      component.onSetupMirroring();

      expect(cephfsServiceMock.createBootstrapPeer).toHaveBeenCalledWith(
        'myfs',
        'eyJrZXkiOiJ2YWx1ZSJ9'
      );
    });

    it('should reset isSubmitting on error without emitting', () => {
      const emitSpy = jest.spyOn(component.mirroringSetup, 'emit');
      const setErrorsSpy = jest.spyOn(component.setupForm, 'setErrors');
      taskWrapperMock.wrapTaskAroundCall.mockReturnValue(throwError(() => new Error('fail')));
      component.setupForm.setValue({ filesystem: 'myfs', token: 'eyJrZXkiOiJ2YWx1ZSJ9' });

      component.onSetupMirroring();

      expect(component.isSubmitting).toBe(false);
      expect(setErrorsSpy).toHaveBeenCalledWith({ cdSubmitButton: true });
      expect(emitSpy).not.toHaveBeenCalled();
    });

    it('should set isSubmitting to true before API call completes', () => {
      component.setupForm.setValue({ filesystem: 'myfs', token: 'eyJrZXkiOiJ2YWx1ZSJ9' });

      const submittingDuringCall: boolean[] = [];
      taskWrapperMock.wrapTaskAroundCall.mockImplementation(({ call }) => {
        submittingDuringCall.push(component.isSubmitting);
        return call;
      });

      component.onSetupMirroring();
      expect(submittingDuringCall[0]).toBe(true);
    });
  });

  describe('closeModal', () => {
    beforeEach(() => fixture.detectChanges());

    it('should emit close and reset form', () => {
      const closeSpy = jest.spyOn(component.close, 'emit');
      component.setupForm.setValue({ filesystem: 'myfs', token: 'eyJrZXkiOiJ2YWx1ZSJ9' });
      component.isSubmitting = true;

      component.closeModal();

      expect(closeSpy).toHaveBeenCalled();
      expect(component.isSubmitting).toBe(false);
      expect(component.setupForm.value).toEqual({ filesystem: null, token: null });
    });
  });
});
