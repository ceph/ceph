import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserTabsComponent } from './rgw-user-tabs.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwUserTabsComponent', () => {
  let component: RgwUserTabsComponent;
  let fixture: ComponentFixture<RgwUserTabsComponent>;

  configureTestBed({
    declarations: [RgwUserTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
