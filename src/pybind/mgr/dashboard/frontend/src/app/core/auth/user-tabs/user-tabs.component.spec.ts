import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'carbon-components-angular';

import { configureTestBed } from '~/testing/unit-test-helper';
import { UserTabsComponent } from './user-tabs.component';

describe('UserTabsComponent', () => {
  let component: UserTabsComponent;
  let fixture: ComponentFixture<UserTabsComponent>;
  let router: Router;
  let navigateSpy: jest.SpyInstance;

  configureTestBed({
    imports: [RouterTestingModule, TabsModule],
    declarations: [UserTabsComponent]
  });

  const createComponent = (url: string) => {
    router = TestBed.inject(Router);
    jest.spyOn(router, 'url', 'get').mockReturnValue(url);
    navigateSpy = jest.spyOn(router, 'navigate').mockResolvedValue(true);
    fixture = TestBed.createComponent(UserTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  it('should create', () => {
    createComponent('/user-management/users');
    expect(component).toBeTruthy();
  });

  it('should render the Users and Roles tabs', () => {
    createComponent('/user-management/users');
    const tabs = fixture.debugElement.queryAll(By.css('cds-tab'));
    expect(tabs.map((tab) => tab.attributes['heading'])).toEqual(['Users', 'Roles']);
  });

  it('should render the tabs as navigation', () => {
    createComponent('/user-management/users');
    const tabs = fixture.debugElement.query(By.css('cds-tabs')).componentInstance;
    expect(tabs.isNavigation).toBeTruthy();
    expect(tabs.cacheActive).toBeFalsy();
  });

  it('should activate the Users tab for the users url', () => {
    createComponent('/user-management/users');
    expect(component.activeTab).toBe(component.Tabs.users);
  });

  it('should activate the Roles tab for the roles url', () => {
    createComponent('/user-management/roles');
    expect(component.activeTab).toBe(component.Tabs.roles);
  });

  it('should not navigate when the active tab is selected', () => {
    createComponent('/user-management/users');
    component.onSelected(component.Tabs.users);
    expect(navigateSpy).not.toHaveBeenCalled();
  });

  it('should navigate when another tab is selected', () => {
    createComponent('/user-management/users');
    component.onSelected(component.Tabs.roles);
    expect(navigateSpy).toHaveBeenCalledWith(['/user-management/roles']);
    expect(component.activeTab).toBe(component.Tabs.roles);
  });
});
