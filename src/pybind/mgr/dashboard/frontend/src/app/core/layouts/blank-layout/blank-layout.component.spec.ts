import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BlankLayoutComponent } from './blank-layout.component';

describe('DefaultLayoutComponent', () => {
  let component: BlankLayoutComponent;
  let fixture: ComponentFixture<BlankLayoutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [BlankLayoutComponent],
      imports: [RouterTestingModule]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BlankLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
