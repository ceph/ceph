import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { IscsiTabsComponent } from './iscsi-tabs.component';

describe('IscsiTabsComponent', () => {
  let component: IscsiTabsComponent;
  let fixture: ComponentFixture<IscsiTabsComponent>;

  configureTestBed({
    imports: [SharedModule, RouterTestingModule, NgbNavModule],
    declarations: [IscsiTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
