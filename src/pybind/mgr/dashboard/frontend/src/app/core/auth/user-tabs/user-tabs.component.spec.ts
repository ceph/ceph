import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { UserTabsComponent } from './user-tabs.component';
import { By } from '@angular/platform-browser';

describe('UserTabsComponent', () => {
  let component: UserTabsComponent;
  let fixture: ComponentFixture<UserTabsComponent>;

  configureTestBed({
    imports: [SharedModule, RouterTestingModule, HttpClientTestingModule],
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

  it('should display the heading text in the tab', () => {
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.length).toBe(2);
    expect(tabs[0].attributes['heading']).toBe('Users');
    expect(tabs[1].attributes['heading']).toBe('Roles');
  });
});
