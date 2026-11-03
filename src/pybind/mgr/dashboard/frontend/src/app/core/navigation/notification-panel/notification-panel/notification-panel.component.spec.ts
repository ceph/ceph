import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { NotificationPanelComponent } from './notification-panel.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';

describe('NotificationPanelComponent', () => {
  let component: NotificationPanelComponent;
  let fixture: ComponentFixture<NotificationPanelComponent>;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SharedModule, HttpClientTestingModule],
      declarations: [NotificationPanelComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    notificationService = TestBed.inject(NotificationService);
    fixture = TestBed.createComponent(NotificationPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should close panel on click outside', () => {
    spyOn(notificationService, 'setPanelState');
    const outsideEl = document.createElement('div');
    document.body.appendChild(outsideEl);

    component.onClickOutside(outsideEl);

    expect(notificationService.setPanelState).toHaveBeenCalledWith(false);
    outsideEl.remove();
  });

  it('should not close panel on click inside', () => {
    spyOn(notificationService, 'setPanelState');

    component.onClickOutside(fixture.nativeElement);

    expect(notificationService.setPanelState).not.toHaveBeenCalled();
  });

  it('should not close panel when clicking the notification bell icon', () => {
    spyOn(notificationService, 'setPanelState');
    const bellEl = document.createElement('div');
    bellEl.setAttribute('data-testid', 'header-notification-icon');
    document.body.appendChild(bellEl);

    component.onClickOutside(bellEl);

    expect(notificationService.setPanelState).not.toHaveBeenCalled();
    bellEl.remove();
  });

  it('should close panel on Escape key', () => {
    spyOn(notificationService, 'setPanelState');

    component.onEscape();

    expect(notificationService.setPanelState).toHaveBeenCalledWith(false);
  });
});
