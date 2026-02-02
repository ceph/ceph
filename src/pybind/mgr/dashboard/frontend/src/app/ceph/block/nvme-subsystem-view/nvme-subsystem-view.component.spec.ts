import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NvmeSubsystemViewComponent } from './nvme-subsystem-view.component';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('NvmeSubsystemViewComponent', () => {
  let component: NvmeSubsystemViewComponent;
  let fixture: ComponentFixture<NvmeSubsystemViewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeSubsystemViewComponent],
      imports: [RouterTestingModule, SharedModule, HttpClientTestingModule]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeSubsystemViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
