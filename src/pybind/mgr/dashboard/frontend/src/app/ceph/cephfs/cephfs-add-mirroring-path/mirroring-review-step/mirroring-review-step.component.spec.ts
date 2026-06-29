import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { MirroringReviewStepComponent } from './mirroring-review-step.component';

describe('MirroringReviewStepComponent', () => {
  let component: MirroringReviewStepComponent;
  let fixture: ComponentFixture<MirroringReviewStepComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [MirroringReviewStepComponent],
      imports: [ReactiveFormsModule],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(MirroringReviewStepComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(MirroringReviewStepComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize an empty form group on ngOnInit', () => {
    component.ngOnInit();

    expect(component.formGroup).toBeTruthy();
  });

  it('should have default input values', () => {
    expect(component.destinationCluster).toBe('—');
    expect(component.destinationFilesystem).toBe('—');
    expect(component.totalPaths).toBe(0);
    expect(component.snapshotInterval).toBe('—');
    expect(component.retention).toBe('—');
    expect(component.existingScheduleCount).toBe(0);
  });

  it('should accept input values', () => {
    component.destinationCluster = 'remote-cluster';
    component.destinationFilesystem = 'remote-fs';
    component.totalPaths = 3;
    component.snapshotInterval = 'Every day';
    component.retention = '7 daily';
    component.existingScheduleCount = 2;

    expect(component.destinationCluster).toBe('remote-cluster');
    expect(component.destinationFilesystem).toBe('remote-fs');
    expect(component.totalPaths).toBe(3);
    expect(component.snapshotInterval).toBe('Every day');
    expect(component.retention).toBe('7 daily');
    expect(component.existingScheduleCount).toBe(2);
  });
});
