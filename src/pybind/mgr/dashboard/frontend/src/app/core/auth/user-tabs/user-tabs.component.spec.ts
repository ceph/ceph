import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { UserTabsComponent } from './user-tabs.component';

describe('UserTabsComponent', () => {
  let component: UserTabsComponent;
  let fixture: ComponentFixture<UserTabsComponent>;

  configureTestBed({
    imports: [SharedModule, RouterTestingModule, HttpClientTestingModule, NgbNavModule],
    declarations: [UserTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
