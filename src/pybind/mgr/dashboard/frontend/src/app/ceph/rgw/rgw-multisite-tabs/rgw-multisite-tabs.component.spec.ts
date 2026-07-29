import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteTabsComponent } from './rgw-multisite-tabs.component';

describe('RgwMultisiteTabsComponent', () => {
  let component: RgwMultisiteTabsComponent;
  let fixture: ComponentFixture<RgwMultisiteTabsComponent>;

  configureTestBed({
    declarations: [RgwMultisiteTabsComponent]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(RgwMultisiteTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
