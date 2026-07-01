import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { MirroringReviewStepComponent } from './mirroring-review-step.component';

describe('MirroringReviewStepComponent', () => {
  let component: MirroringReviewStepComponent;
  let fixture: ComponentFixture<MirroringReviewStepComponent>;

  const cephfsServiceMock = {
    listDaemonStatus: jest.fn().mockReturnValue(
      of([
        {
          daemon_id: 1,
          filesystems: [
            {
              name: 'testfs',
              peers: [
                {
                  remote: {
                    cluster_name: 'remote-cluster',
                    fs_name: 'remote-fs'
                  }
                }
              ]
            }
          ]
        }
      ])
    )
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MirroringReviewStepComponent],
      providers: [{ provide: CephfsService, useValue: cephfsServiceMock }],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(MirroringReviewStepComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should have an empty formGroup', () => {
    fixture.detectChanges();
    expect(component.formGroup).toBeDefined();
    expect(Object.keys(component.formGroup.controls)).toHaveLength(0);
  });

  it('should load destination info from daemon status', () => {
    component.fsName = 'testfs';
    fixture.detectChanges();

    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalled();
    expect(component.destinationCluster).toBe('remote-cluster');
    expect(component.destinationFilesystem).toBe('remote-fs');
  });

  it('should derive review values from wizard steps', () => {
    component.pathsStep = {
      getSubmitPaths: () => ({ toAdd: ['/volumes/a', '/volumes/b'], alreadyMirrored: [] })
    } as any;
    component.scheduleStep = {
      snapScheduleForm: {
        getRawValue: () => ({
          repeatInterval: 2,
          repeatFrequency: 'h',
          retentionPolicies: [{ retentionInterval: 7, retentionFrequency: 'd' }]
        })
      }
    } as any;
    fixture.detectChanges();

    expect(component.totalPaths).toBe(2);
    expect(component.snapshotInterval).toContain('2');
    expect(component.retention).toBe('7 Daily');
  });
});
