import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { RbdTabsComponent } from './rbd-tabs.component';

describe('RbdTabsComponent', () => {
  let component: RbdTabsComponent;
  let fixture: ComponentFixture<RbdTabsComponent>;

  configureTestBed({
    imports: [TabsModule.forRoot(), RouterTestingModule],
    declarations: [RbdTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
