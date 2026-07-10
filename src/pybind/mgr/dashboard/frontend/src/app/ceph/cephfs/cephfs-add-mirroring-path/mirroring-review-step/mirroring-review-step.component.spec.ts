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
    expect(component.formGroup.valid).toBe(true);
  });

  it('should expose paths to add from the paths step', () => {
    component.pathsStep = {
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1', '/volumes/g1/sv1'],
        alreadyMirrored: []
      })
    } as any;

    expect(component.pathsToAdd).toEqual(['/volumes/g1', '/volumes/g1/sv1']);
  });

  it('should return an empty paths list when the paths step is missing', () => {
    component.pathsStep = undefined;

    expect(component.pathsToAdd).toEqual([]);
  });

  it('should expose the schedule summary from the schedule step', () => {
    component.scheduleStep = {
      getScheduleSummary: () => 'Every 1h · Retention: 7-d'
    } as any;

    expect(component.scheduleSummary).toBe('Every 1h · Retention: 7-d');
  });

  it('should return an empty schedule summary when the schedule step is missing', () => {
    component.scheduleStep = undefined;

    expect(component.scheduleSummary).toBe('');
  });
});
