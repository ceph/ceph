import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';

import { CephfsSetupMirroringComponent } from './cephfs-setup-mirroring.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

describe('CephfsSetupMirroringComponent', () => {
  let component: CephfsSetupMirroringComponent;
  let fixture: ComponentFixture<CephfsSetupMirroringComponent>;

  const cephfsServiceMock = {
    list: jest.fn().mockReturnValue(of([])),
    enableMirror: jest.fn().mockReturnValue(of(null)),
    createBootstrapPeer: jest.fn().mockReturnValue(of(null))
  };

  const taskWrapperMock = {
    wrapTaskAroundCall: jest.fn().mockImplementation(({ call }) => call)
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    cephfsServiceMock.list.mockReturnValue(of([]));
    cephfsServiceMock.enableMirror.mockReturnValue(of(null));
    cephfsServiceMock.createBootstrapPeer.mockReturnValue(of(null));

    await TestBed.configureTestingModule({
      declarations: [CephfsSetupMirroringComponent],
      imports: [ReactiveFormsModule],
      providers: [
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

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
      expect(component.filesystems).toEqual([
        { id: 1, name: 'myfs' },
        { id: 2, name: 'backup' }
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
      component.setupForm.setValue({ filesystem: 'myfs', token: 'abc123' });
      expect(component.setupForm.valid).toBe(true);
    });

    it('should be invalid when filesystem is missing', () => {
      component.setupForm.setValue({ filesystem: '', token: 'abc123' });
      expect(component.setupForm.invalid).toBe(true);
    });

    it('should be invalid when token is missing', () => {
      component.setupForm.setValue({ filesystem: 'myfs', token: '' });
      expect(component.setupForm.invalid).toBe(true);
    });
  });

  describe('onSetupMirroring', () => {
    beforeEach(() => fixture.detectChanges());

    it('should not submit when form is invalid', () => {
      component.onSetupMirroring();
      expect(taskWrapperMock.wrapTaskAroundCall).not.toHaveBeenCalled();
    });

    it('should call enableMirror then createBootstrapPeer and emit on success', () => {
      const emitSpy = jest.spyOn(component.mirroringSetup, 'emit');
      component.setupForm.setValue({ filesystem: 'myfs', token: '  tok123  ' });

      component.onSetupMirroring();

      expect(cephfsServiceMock.enableMirror).toHaveBeenCalledWith('myfs');
      expect(cephfsServiceMock.createBootstrapPeer).toHaveBeenCalledWith('myfs', 'tok123');
      expect(taskWrapperMock.wrapTaskAroundCall).toHaveBeenCalledWith(
        expect.objectContaining({
          task: expect.objectContaining({ name: 'mirroring/setup' })
        })
      );
      expect(component.isSubmitting).toBe(false);
      expect(emitSpy).toHaveBeenCalledWith({ filesystem: 'myfs' });
    });

    it('should still emit and reset isSubmitting on error', () => {
      const emitSpy = jest.spyOn(component.mirroringSetup, 'emit');
      taskWrapperMock.wrapTaskAroundCall.mockReturnValue(throwError(() => new Error('fail')));
      component.setupForm.setValue({ filesystem: 'myfs', token: 'tok' });

      component.onSetupMirroring();

      expect(component.isSubmitting).toBe(false);
      expect(emitSpy).toHaveBeenCalledWith({ filesystem: 'myfs' });
    });

    it('should set isSubmitting to true before API call completes', () => {
      taskWrapperMock.wrapTaskAroundCall.mockReturnValue(of(null));
      component.setupForm.setValue({ filesystem: 'myfs', token: 'tok' });

      const submittingDuringCall: boolean[] = [];
      const origWrap = taskWrapperMock.wrapTaskAroundCall;
      taskWrapperMock.wrapTaskAroundCall.mockImplementation((opts: any) => {
        submittingDuringCall.push(component.isSubmitting);
        return origWrap(opts);
      });

      component.onSetupMirroring();
      expect(submittingDuringCall[0]).toBe(true);
    });
  });

  describe('onCancel', () => {
    beforeEach(() => fixture.detectChanges());

    it('should emit cancelled and reset form', () => {
      const cancelledSpy = jest.spyOn(component.cancelled, 'emit');
      component.setupForm.setValue({ filesystem: 'myfs', token: 'tok' });
      component.isSubmitting = true;

      component.onCancel();

      expect(cancelledSpy).toHaveBeenCalled();
      expect(component.isSubmitting).toBe(false);
      expect(component.setupForm.value).toEqual({ filesystem: null, token: null });
    });
  });
});
