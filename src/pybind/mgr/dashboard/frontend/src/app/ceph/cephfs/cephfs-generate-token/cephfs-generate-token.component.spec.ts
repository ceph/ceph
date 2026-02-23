import {
  ComponentFixture,
  TestBed,
  fakeAsync,
  tick
} from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';
import { SimpleChange } from '@angular/core';

import { CephfsGenerateTokenComponent } from './cephfs-generate-token.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { HealthService } from '~/app/shared/api/health.service';

describe('CephfsGenerateTokenComponent', () => {
  let component: CephfsGenerateTokenComponent;
  let fixture: ComponentFixture<CephfsGenerateTokenComponent>;

  let cephfsServiceMock: any;
  let taskWrapperServiceMock: any;
  let healthServiceMock: any;

  beforeEach(async () => {
    cephfsServiceMock = {
      createBootstrapToken: jest.fn()
    };

    taskWrapperServiceMock = {
      wrapTaskAroundCall: jest.fn()
    };

    healthServiceMock = {
      getHealthSnapshot: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsGenerateTokenComponent],
      imports: [ReactiveFormsModule],
      providers: [
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperServiceMock },
        { provide: HealthService, useValue: healthServiceMock }
      ]
    })
      .overrideComponent(CephfsGenerateTokenComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsGenerateTokenComponent);
    component = fixture.componentInstance;
    healthServiceMock.getHealthSnapshot.mockReturnValue(
      of({ fsid: 'cluster-123' })
    );
    taskWrapperServiceMock.wrapTaskAroundCall.mockImplementation(
      ({ call }) => call
    );

    fixture.detectChanges();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create component', () => {
    expect(component).toBeTruthy();
  });

  it('should set clusterId on init', () => {
    expect(component.clusterId).toBe('cluster-123');
  });

  it('should update filesystemName on selectedFilesystem change', () => {
    component.selectedFilesystem = { name: 'fs1' } as any;
    component.ngOnChanges({
      selectedFilesystem: new SimpleChange(null, component.selectedFilesystem, true)
    });
    expect(component.filesystemName).toBe('fs1');
    expect(component.generatedToken).toBe('');
    expect(component.siteName.value).toBe('');
  });

  it('should update entityName when selectedEntity is string', () => {
    component.selectedEntity = 'client.test';
    component.ngOnChanges({
      selectedEntity: new SimpleChange(null, 'client.test', true)
    });
    expect(component.entityName).toBe('client.test');
  });

  it('should update entityName when selectedEntity is object', () => {
    component.selectedEntity = { entity: 'client.obj' } as any;
    component.ngOnChanges({
      selectedEntity: new SimpleChange(null, component.selectedEntity, true)
    });
    expect(component.entityName).toBe('client.obj');
  });

  it('should not generate if siteName invalid', () => {
    component.siteName.setValue('');
    component.onGenerateToken();
    expect(cephfsServiceMock.createBootstrapToken).not.toHaveBeenCalled();
  });

  it('should not generate if filesystem or entity missing', () => {
    component.siteName.setValue('site1');
    component.filesystemName = '';
    component.entityName = '';
    component.onGenerateToken();
    expect(cephfsServiceMock.createBootstrapToken).not.toHaveBeenCalled();
  });

  it('should generate token successfully (object response)', fakeAsync(() => {
    component.filesystemName = 'fs1';
    component.entityName = 'client1';
    component.siteName.setValue('site1');
    cephfsServiceMock.createBootstrapToken.mockReturnValue(
      of({ token: 'abc123' })
    );
    component.onGenerateToken();
    tick();

    expect(component.generatedToken).toBe('abc123');
    expect(component.isGenerating).toBeFalsy();
  }));

  it('should generate token successfully (string response)', fakeAsync(() => {
    component.filesystemName = 'fs1';
    component.entityName = 'client1';
    component.siteName.setValue('site1');

    cephfsServiceMock.createBootstrapToken.mockReturnValue(
      of('raw-token')
    );
    component.onGenerateToken();
    tick();

    expect(component.generatedToken).toBe('raw-token');
    expect(component.isGenerating).toBeFalsy();
  }));

  it('should handle generation error', fakeAsync(() => {
    component.filesystemName = 'fs1';
    component.entityName = 'client1';
    component.siteName.setValue('site1');
    cephfsServiceMock.createBootstrapToken.mockReturnValue(
      throwError(() => new Error('fail'))
    );
    component.onGenerateToken();
    tick();

    expect(component.isGenerating).toBeFalsy();
  }));

  it('should download token when generatedToken exists', () => {
    component.generatedToken = 'download-token';
    component.siteName.setValue('mysite');
    const createSpy = jest.spyOn(document, 'createElement');
    component.downloadToken();
    expect(createSpy).toHaveBeenCalledWith('a');
  });

  it('should dismiss alerts properly', () => {
    component.onDismissSuccessAlert();
    expect(component.showSuccessAlert).toBeFalsy();

    component.onDismissWarningAlert();
    expect(component.showWarningAlert).toBeFalsy();

    component.onDismissInfoAlert();
    expect(component.showInfoAlert).toBeFalsy();

    component.onDismissEnvironmentAlert();
    expect(component.showEnvironmentAlert).toBeFalsy();
  });
});