import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MirroringReviewStepComponent } from './mirroring-review-step.component';

describe('MirroringReviewStepComponent', () => {
  let component: MirroringReviewStepComponent;
  let fixture: ComponentFixture<MirroringReviewStepComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MirroringReviewStepComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(MirroringReviewStepComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialise with default values', () => {
    expect(component.fsName).toBe('—');
    expect(component.totalPaths).toBe(0);
    expect(component.snapshotInterval).toBe('—');
    expect(component.retention).toBe('—');
  });

  it('should have an empty formGroup', () => {
    expect(component.formGroup).toBeDefined();
    expect(Object.keys(component.formGroup.controls)).toHaveLength(0);
  });

  it('should compute totalPaths from pathsStep', () => {
    component.pathsStep = { getSubmitPaths: () => ({ toAdd: ['/a', '/b'], alreadyMirrored: [] }) } as any;
    expect(component.totalPaths).toBe(2);
  });

  it('should compute snapshotInterval from scheduleStep', () => {
    component.scheduleStep = {
      snapScheduleForm: {
        get: (key: string) => {
          if (key === 'repeatInterval') return { value: 2 };
          if (key === 'repeatFrequency') return { value: 'h' };
          return { value: null };
        }
      }
    } as any;
    expect(component.snapshotInterval).toBe('2 hours');
  });

  it('should compute retention from scheduleStep', () => {
    component.scheduleStep = {
      snapScheduleForm: {
        get: () => ({ value: null })
      },
      retentionPolicies: {
        controls: [
          {
            get: (key: string) => {
              if (key === 'retentionInterval') return { value: 7 };
              if (key === 'retentionFrequency') return { value: 'd' };
              return { value: null };
            }
          }
        ]
      }
    } as any;
    expect(component.retention).toBe('7 Daily');
  });
});
