import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { WizardComponent } from './wizard.component';

describe('WizardComponent', () => {
  let component: WizardComponent;
  let fixture: ComponentFixture<WizardComponent>;

  configureTestBed({
    imports: [SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WizardComponent);
    component = fixture.componentInstance;
    component.stepsTitle = [
      {
        label: 'Add Hosts'
      },
      {
        label: 'Create OSDs',
        complete: false
      }
    ];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
