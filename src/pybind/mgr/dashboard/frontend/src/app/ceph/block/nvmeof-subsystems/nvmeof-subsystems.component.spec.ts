import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofSubsystemsComponent } from './nvmeof-subsystems.component';

describe('NvmeofSubsystemsComponent', () => {
  let component: NvmeofSubsystemsComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
