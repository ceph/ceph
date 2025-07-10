import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { IconComponent } from './icon.component';

describe('IconComponent', () => {
  let component: IconComponent;
  let fixture: ComponentFixture<IconComponent>;

  configureTestBed({
    declarations: [IconComponent],
    imports: []
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
