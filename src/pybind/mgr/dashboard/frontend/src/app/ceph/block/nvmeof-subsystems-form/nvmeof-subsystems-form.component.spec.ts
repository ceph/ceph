import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofSubsystemsFormComponent } from './nvmeof-subsystems-form.component';

describe('NvmeofSubsystemsFormComponent', () => {
  let component: NvmeofSubsystemsFormComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
