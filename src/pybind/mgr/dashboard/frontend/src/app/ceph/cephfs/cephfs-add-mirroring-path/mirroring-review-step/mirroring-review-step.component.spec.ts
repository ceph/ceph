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
    expect(component.destinationCluster).toBe('—');
    expect(component.destinationFilesystem).toBe('—');
    expect(component.totalPaths).toBe(0);
    expect(component.snapshotInterval).toBe('—');
    expect(component.retention).toBe('—');
    expect(component.existingScheduleCount).toBe(0);
  });

  it('should have an empty formGroup', () => {
    expect(component.formGroup).toBeDefined();
    expect(Object.keys(component.formGroup.controls)).toHaveLength(0);
  });

  it('should accept input values', () => {
    component.destinationCluster = 'remote-cluster';
    component.destinationFilesystem = 'remote-fs';
    component.totalPaths = 3;
    component.snapshotInterval = 'Every 2 hours';
    component.retention = '7 daily';
    component.existingScheduleCount = 5;
    fixture.detectChanges();

    expect(component.destinationCluster).toBe('remote-cluster');
    expect(component.destinationFilesystem).toBe('remote-fs');
    expect(component.totalPaths).toBe(3);
    expect(component.snapshotInterval).toBe('Every 2 hours');
    expect(component.retention).toBe('7 daily');
    expect(component.existingScheduleCount).toBe(5);
  });
});
