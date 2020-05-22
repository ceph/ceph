import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { RbdTabsComponent } from './rbd-tabs.component';

describe('RbdTabsComponent', () => {
  let component: RbdTabsComponent;
  let fixture: ComponentFixture<RbdTabsComponent>;

  configureTestBed({
    imports: [RouterTestingModule, NgbNavModule],
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
