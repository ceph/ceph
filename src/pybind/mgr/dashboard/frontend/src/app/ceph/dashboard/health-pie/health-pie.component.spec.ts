import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { HealthPieComponent } from './health-pie.component';

describe('HealthPieComponent', () => {
  let component: HealthPieComponent;
  let fixture: ComponentFixture<HealthPieComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [HealthPieComponent],
      providers: [DimlessBinaryPipe, FormatterService]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthPieComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
