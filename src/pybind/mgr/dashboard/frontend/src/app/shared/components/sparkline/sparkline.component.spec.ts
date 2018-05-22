import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SparklineComponent } from './sparkline.component';

describe('SparklineComponent', () => {
  let component: SparklineComponent;
  let fixture: ComponentFixture<SparklineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [SparklineComponent],
      schemas: [NO_ERRORS_SCHEMA],
      imports: []
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SparklineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
