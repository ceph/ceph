import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { IconComponent } from './icon.component';

@Component({
  template: `<cd-icon type="success">Status text</cd-icon>`,
  standalone: false
})
class TestHostComponent {}

describe('IconComponent', () => {
  let component: IconComponent;
  let fixture: ComponentFixture<IconComponent>;

  configureTestBed({
    declarations: [IconComponent, TestHostComponent],
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

  it('should render projected text', () => {
    const hostFixture = TestBed.createComponent(TestHostComponent);
    hostFixture.detectChanges();

    expect(hostFixture.nativeElement.textContent).toContain('Status text');
  });
});
