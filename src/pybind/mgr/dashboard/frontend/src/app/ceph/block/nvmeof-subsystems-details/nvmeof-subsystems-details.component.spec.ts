import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofSubsystemsDetailsComponent } from './nvmeof-subsystems-details.component';

describe('NvmeofSubsystemsDetailsComponent', () => {
  let component: NvmeofSubsystemsDetailsComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
