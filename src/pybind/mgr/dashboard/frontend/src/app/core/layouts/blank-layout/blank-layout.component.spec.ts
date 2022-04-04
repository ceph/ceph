import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BlankLayoutComponent } from './blank-layout.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('DefaultLayoutComponent', () => {
  let component: BlankLayoutComponent;
  let fixture: ComponentFixture<BlankLayoutComponent>;

  configureTestBed({
    declarations: [BlankLayoutComponent],
    imports: [RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BlankLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
