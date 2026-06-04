import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsGenerateTokenComponent } from './cephfs-generate-token.component';

describe('CephfsGenerateTokenComponent', () => {
  let component: CephfsGenerateTokenComponent;
  let fixture: ComponentFixture<CephfsGenerateTokenComponent>;

  const cephfsServiceMock = {
    list: jest.fn().mockReturnValue(of([])),
    enableMirror: jest.fn().mockReturnValue(of(null)),
    createBootstrapToken: jest.fn().mockReturnValue(of({ token: 'test-token' }))
  };

  const clusterServiceMock = {
    listUser: jest.fn().mockReturnValue(of([]))
  };

  const taskWrapperMock = {
    wrapTaskAroundCall: jest.fn().mockImplementation(({ call }) => call)
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [CephfsGenerateTokenComponent],
      imports: [ReactiveFormsModule],
      providers: [
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: ClusterService, useValue: clusterServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsGenerateTokenComponent);
    component = fixture.componentInstance;
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize the token form with required validators', () => {
    expect(component.tokenForm).toBeTruthy();
    expect(component.tokenForm.controls['filesystem']).toBeTruthy();
    expect(component.tokenForm.controls['username']).toBeTruthy();
    expect(component.tokenForm.controls['sitename']).toBeTruthy();

    expect(component.tokenForm.controls['filesystem'].hasError('required')).toBe(true);
    expect(component.tokenForm.controls['username'].hasError('required')).toBe(true);
    expect(component.tokenForm.controls['sitename'].hasError('required')).toBe(true);
  });

  it('should load filesystems on init', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(
      of([
        { id: 1, mdsmap: { fs_name: 'myfs' } },
        { id: 2, mdsmap: { fs_name: 'otherfs' } }
      ])
    );

    component.ngOnInit();
    tick();

    expect(component.filesystems).toEqual([
      { id: 1, name: 'myfs' },
      { id: 2, name: 'otherfs' }
    ]);
  }));

  it('should fallback to fs-<id> when mdsmap has no fs_name', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([{ id: 5, mdsmap: {} }]));

    component.ngOnInit();
    tick();

    expect(component.filesystems[0].name).toBe('fs-5');
  }));

  it('should not generate token when form is invalid', () => {
    component.onGenerateToken();

    expect(component.isGenerating).toBe(false);
    expect(cephfsServiceMock.enableMirror).not.toHaveBeenCalled();
  });

  it('should enable mirroring and create bootstrap token', fakeAsync(() => {
    jest.spyOn(component.tokenGenerated, 'emit');
    component.tokenForm.patchValue({
      filesystem: 'myfs',
      username: 'mirror-peer',
      sitename: 'site-a'
    });

    component.onGenerateToken();
    tick();

    expect(cephfsServiceMock.enableMirror).toHaveBeenCalledWith('myfs');
    expect(cephfsServiceMock.createBootstrapToken).toHaveBeenCalledWith(
      'myfs',
      'client.mirror-peer',
      'site-a'
    );
    expect(component.generatedToken).toBe('test-token');
    expect(component.isGenerating).toBe(false);
    expect(component.tokenGenerated.emit).toHaveBeenCalledWith('test-token');
  }));

  it('should abort when enableMirror fails', fakeAsync(() => {
    cephfsServiceMock.enableMirror.mockReturnValue(throwError(() => new Error('enable failed')));
    jest.spyOn(component.tokenGenerated, 'emit');
    component.tokenForm.patchValue({
      filesystem: 'myfs',
      username: 'mirror-peer',
      sitename: 'site-a'
    });

    component.onGenerateToken();
    tick();

    expect(cephfsServiceMock.createBootstrapToken).not.toHaveBeenCalled();
    expect(component.generatedToken).toBe('');
    expect(component.isGenerating).toBe(false);
    expect(component.tokenGenerated.emit).not.toHaveBeenCalled();
  }));

  it('should not emit token when bootstrap response has no token', fakeAsync(() => {
    cephfsServiceMock.createBootstrapToken.mockReturnValue(of({}));
    jest.spyOn(component.tokenGenerated, 'emit');
    component.tokenForm.patchValue({
      filesystem: 'myfs',
      username: 'mirror-peer',
      sitename: 'site-a'
    });

    component.onGenerateToken();
    tick();

    expect(component.generatedToken).toBe('');
    expect(component.isGenerating).toBe(false);
    expect(component.tokenGenerated.emit).not.toHaveBeenCalled();
  }));

  it('should filter users by MDS capabilities when a filesystem is selected', fakeAsync(() => {
    clusterServiceMock.listUser.mockReturnValue(
      of([
        { entity: 'client.admin', caps: { mds: 'allow *' } },
        { entity: 'client.mirror-myfs', caps: { mds: 'allow r fsname=myfs' } },
        { entity: 'client.mirror-other', caps: { mds: 'allow r fsname=otherfs' } }
      ])
    );

    component.ngOnInit();
    tick();

    component.tokenForm.controls['filesystem'].setValue('myfs');
    tick();

    expect(component.filteredUsers).toEqual(['mirror-myfs']);
  }));

  it('should show no users when no filesystem is selected', fakeAsync(() => {
    clusterServiceMock.listUser.mockReturnValue(
      of([
        { entity: 'client.user1', caps: { mds: 'allow r fsname=fs1' } },
        { entity: 'client.user2', caps: { mds: 'allow r fsname=fs2' } }
      ])
    );

    component.ngOnInit();
    tick();

    component.tokenForm.controls['filesystem'].setValue('');
    tick();

    expect(component.filteredUsers).toEqual([]);
  }));

  it('should emit cancelled and reset state on cancel', () => {
    jest.spyOn(component.cancelled, 'emit');
    component.generatedToken = 'some-token';
    component.isGenerating = true;
    component.tokenForm.patchValue({ filesystem: 'fs1', username: 'user1' });

    component.onCancel();

    expect(component.cancelled.emit).toHaveBeenCalled();
    expect(component.generatedToken).toBe('');
    expect(component.isGenerating).toBe(false);
  });
});
