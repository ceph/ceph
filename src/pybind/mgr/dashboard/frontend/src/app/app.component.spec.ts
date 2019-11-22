import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SidebarModule } from 'ng-sidebar';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../testing/unit-test-helper';
import { AppComponent } from './app.component';
import { RbdService } from './shared/api/rbd.service';
import { PipesModule } from './shared/pipes/pipes.module';
import { AuthStorageService } from './shared/services/auth-storage.service';
import { NotificationService } from './shared/services/notification.service';

describe('AppComponent', () => {
  let component: AppComponent;
  let fixture: ComponentFixture<AppComponent>;

  configureTestBed({
    imports: [
      RouterTestingModule,
      ToastrModule.forRoot(),
      PipesModule,
      HttpClientTestingModule,
      SidebarModule.forRoot()
    ],
    declarations: [AppComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [AuthStorageService, i18nProviders, RbdService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AppComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Sidebar', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      notificationService = TestBed.get(NotificationService);
    });

    it('should always close if sidebarSubject value is true', () => {
      // Closed before next value
      expect(component.sidebarOpened).toBeFalsy();
      notificationService.sidebarSubject.next(true);
      expect(component.sidebarOpened).toBeFalsy();

      // Opened before next value
      component.sidebarOpened = true;
      expect(component.sidebarOpened).toBeTruthy();
      notificationService.sidebarSubject.next(true);
      expect(component.sidebarOpened).toBeFalsy();
    });

    it('should toggle sidebar visibility if sidebarSubject value is false', () => {
      // Closed before next value
      expect(component.sidebarOpened).toBeFalsy();
      notificationService.sidebarSubject.next(false);
      expect(component.sidebarOpened).toBeTruthy();

      // Opened before next value
      component.sidebarOpened = true;
      expect(component.sidebarOpened).toBeTruthy();
      notificationService.sidebarSubject.next(false);
      expect(component.sidebarOpened).toBeFalsy();
    });
  });
});
