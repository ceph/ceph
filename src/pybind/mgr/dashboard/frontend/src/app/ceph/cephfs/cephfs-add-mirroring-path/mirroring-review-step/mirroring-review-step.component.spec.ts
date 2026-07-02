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
    expect(component.selectedPaths).toEqual([]);
    expect(component.snapshotInterval).toBe('—');
    expect(component.retention).toBe('—');
  });

  it('should have an empty formGroup', () => {
    expect(component.formGroup).toBeDefined();
    expect(Object.keys(component.formGroup.controls)).toHaveLength(0);
  });

  it('should accept review summary inputs', () => {
    component.fsName = 'testfs';
    component.totalPaths = 2;
    component.selectedPaths = ['/volumes/g1/sv1', '/volumes/g1/sv2'];
    component.snapshotInterval = '2 weeks';
    component.retention = '7 Daily';
    fixture.detectChanges();

    expect(component.fsName).toBe('testfs');
    expect(component.totalPaths).toBe(2);
    expect(component.selectedPaths).toEqual(['/volumes/g1/sv1', '/volumes/g1/sv2']);
    expect(component.snapshotInterval).toBe('2 weeks');
    expect(component.retention).toBe('7 Daily');
  });
});
