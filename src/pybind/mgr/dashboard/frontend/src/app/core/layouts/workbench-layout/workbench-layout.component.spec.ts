import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { RbdService } from '~/app/shared/api/rbd.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { WorkbenchLayoutComponent } from './workbench-layout.component';

describe('WorkbenchLayoutComponent', () => {
  let component: WorkbenchLayoutComponent;
  let fixture: ComponentFixture<WorkbenchLayoutComponent>;

  configureTestBed({
    imports: [RouterTestingModule, ToastrModule.forRoot(), PipesModule, HttpClientTestingModule],
    declarations: [WorkbenchLayoutComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [AuthStorageService, CssHelper, RbdService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkbenchLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('showTopNotification', () => {
    const notification1 = 'notificationName1';
    const notification2 = 'notificationName2';

    beforeEach(() => {
      component.notifications = [];
    });

    it('should show notification', () => {
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
    });

    it('should not add a second notification if it is already shown', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
    });

    it('should add a second notification if the first one is different', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification2, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.includes(notification2)).toBeTruthy();
      expect(component.notifications.length).toBe(2);
    });

    it('should hide an active notification', () => {
      component.showTopNotification(notification1, true);
      expect(component.notifications.includes(notification1)).toBeTruthy();
      expect(component.notifications.length).toBe(1);
      component.showTopNotification(notification1, false);
      expect(component.notifications.length).toBe(0);
    });

    it('should not fail if it tries to hide an inactive notification', () => {
      expect(() => component.showTopNotification(notification1, false)).not.toThrow();
      expect(component.notifications.length).toBe(0);
    });

    it('should keep other notifications if it hides one', () => {
      component.showTopNotification(notification1, true);
      component.showTopNotification(notification2, true);
      expect(component.notifications.length).toBe(2);
      component.showTopNotification(notification2, false);
      expect(component.notifications.length).toBe(1);
      expect(component.notifications.includes(notification1)).toBeTruthy();
    });
  });
});
