import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteWizardComponent } from './rgw-multisite-wizard.component';

describe('RgwMultisiteWizardComponent', () => {
  let component: RgwMultisiteWizardComponent;
  let fixture: ComponentFixture<RgwMultisiteWizardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteWizardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteWizardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
