import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Component, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { NotificationItemComponent } from './notification-item.component';
import {
  NotificationApplication,
  NotificationType
} from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';

@Component({
  template: `
    <cd-notification-item
      [type]="type"
      [title]="title"
      [timestamp]="timestamp"
      [message]="message"
      [notificationId]="notificationId"
      [application]="application"
      [showChevron]="showChevron"
      [showUnread]="showUnread"
      (deleted)="onDeleted($event)"
    >
    </cd-notification-item>
  `,
  standalone: false
})
class TestHostComponent {
  type = NotificationType.error;
  title = 'Test Title';
  timestamp: Date | string | number = new Date();
  message = 'Test message';
  notificationId = 'test-1';
  application: string;
  showChevron = false;
  showUnread = false;
  deletedId: string = null;

  onDeleted(id: string) {
    this.deletedId = id;
  }
}

describe('NotificationItemComponent', () => {
  let fixture: ComponentFixture<TestHostComponent>;
  let hostComponent: TestHostComponent;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SharedModule, HttpClientTestingModule],
      declarations: [NotificationItemComponent, TestHostComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestHostComponent);
    hostComponent = fixture.componentInstance;
    notificationService = TestBed.inject(NotificationService);
    fixture.detectChanges();
  });

  it('should create', () => {
    const el = fixture.nativeElement.querySelector('cd-notification-item');
    expect(el).toBeTruthy();
  });

  it('should resolve the correct icon for the given type', () => {
    const component = fixture.debugElement.children[0]
      .componentInstance as NotificationItemComponent;
    expect(component.iconMap[component.type]).toBe('error');
  });

  it('should render message as HTML when showChevron is false', () => {
    const msgEl = fixture.nativeElement.querySelector('.cd-notification-item__message');
    expect(msgEl).toBeTruthy();
    expect(msgEl.textContent).toContain('Test message');
  });

  it('should render message as preview when showChevron is true', () => {
    hostComponent.showChevron = true;
    fixture.detectChanges();
    const previewEl = fixture.nativeElement.querySelector('.cd-notification-item__preview');
    expect(previewEl).toBeTruthy();
    expect(previewEl.textContent.trim()).toBe('Test message');
  });

  it('should map all notification types to icons', () => {
    const component = fixture.debugElement.children[0]
      .componentInstance as NotificationItemComponent;
    expect(component.iconMap[NotificationType.success]).toBe('success');
    expect(component.iconMap[NotificationType.error]).toBe('error');
    expect(component.iconMap[NotificationType.info]).toBe('infoCircle');
    expect(component.iconMap[NotificationType.warning]).toBe('warning');
    expect(component.iconMap['default']).toBe('infoCircle');
  });

  it('should render title', () => {
    const titleEl = fixture.nativeElement.querySelector('.cd-notification-item__title');
    expect(titleEl).toBeTruthy();
    expect(titleEl.textContent.trim()).toBe('Test Title');
  });

  it('should always render timestamp at top', () => {
    const contentEl = fixture.nativeElement.querySelector(
      '.cd-notification-item__content .cd-notification-item__timestamp'
    );
    expect(contentEl).toBeTruthy();
  });

  it('should not render timestamp when not provided', () => {
    hostComponent.timestamp = undefined;
    fixture.detectChanges();
    const timestampEls = fixture.nativeElement.querySelectorAll('.cd-notification-item__timestamp');
    expect(timestampEls.length).toBe(0);
  });

  it('should not render title when empty', () => {
    hostComponent.title = '';
    fixture.detectChanges();
    const titleEl = fixture.nativeElement.querySelector('.cd-notification-item__title');
    expect(titleEl).toBeNull();
  });

  it('should show delete button by default when showChevron is false', () => {
    const deleteBtn = fixture.nativeElement.querySelector('.cd-notification-item__delete');
    expect(deleteBtn).toBeTruthy();
  });

  it('should show chevron and hide delete when showChevron is true', () => {
    hostComponent.showChevron = true;
    fixture.detectChanges();
    const chevron = fixture.nativeElement.querySelector('.cd-notification-item__chevron');
    const deleteBtn = fixture.nativeElement.querySelector('.cd-notification-item__delete');
    expect(chevron).toBeTruthy();
    expect(deleteBtn).toBeNull();
  });

  it('should render app badge with Ceph logo', () => {
    hostComponent.application = NotificationApplication.Ceph;
    fixture.detectChanges();
    const appEl = fixture.nativeElement.querySelector('.cd-notification-item__app');
    expect(appEl).toBeTruthy();
    const img = appEl.querySelector('.cd-notification-item__app-icon');
    expect(img.getAttribute('src')).toBe('assets/Ceph_Logo.svg');
    expect(img.getAttribute('alt')).toBe('Ceph');
  });

  it('should render Prometheus logo when application is Prometheus', () => {
    hostComponent.application = NotificationApplication.Prometheus;
    fixture.detectChanges();
    const img = fixture.nativeElement.querySelector('.cd-notification-item__app-icon');
    expect(img.getAttribute('src')).toBe('assets/prometheus_logo.svg');
  });

  it('should not render app badge when application is not set', () => {
    const appEl = fixture.nativeElement.querySelector('.cd-notification-item__app');
    expect(appEl).toBeNull();
  });

  it('should call notificationService.removeById and emit deleted on delete', () => {
    spyOn(notificationService, 'removeById').and.returnValue(true);

    const deleteBtn = fixture.nativeElement.querySelector('.cd-notification-item__delete');
    deleteBtn.click();

    expect(notificationService.removeById).toHaveBeenCalledWith('test-1');
    expect(hostComponent.deletedId).toBe('test-1');
  });

  it('should show dot placeholder (no color) in list panel when read', () => {
    hostComponent.showChevron = true;
    hostComponent.showUnread = false;
    fixture.detectChanges();
    const dot = fixture.nativeElement.querySelector('.cd-notification-item__unread-dot');
    expect(dot).toBeTruthy();
    expect(dot.classList).not.toContain('cd-notification-item__unread-dot--visible');
  });

  it('should show colored unread dot in list panel when unread', () => {
    hostComponent.showChevron = true;
    hostComponent.showUnread = true;
    fixture.detectChanges();
    const dot = fixture.nativeElement.querySelector('.cd-notification-item__unread-dot');
    expect(dot).toBeTruthy();
    expect(dot.classList).toContain('cd-notification-item__unread-dot--visible');
  });

  it('should show severity icon and no dot when not in list panel', () => {
    const dot = fixture.nativeElement.querySelector('.cd-notification-item__unread-dot');
    const icon = fixture.nativeElement.querySelector('cd-icon');
    expect(dot).toBeNull();
    expect(icon).toBeTruthy();
  });

  it('should apply unread title class when showUnread is true', () => {
    hostComponent.showUnread = true;
    fixture.detectChanges();
    const titleEl = fixture.nativeElement.querySelector('.cd-notification-item__title');
    expect(titleEl.classList).toContain('cd-notification-item__title--unread');
  });

  it('should not emit deleted when notification is not found', () => {
    spyOn(notificationService, 'removeById').and.returnValue(false);

    const deleteBtn = fixture.nativeElement.querySelector('.cd-notification-item__delete');
    deleteBtn.click();

    expect(notificationService.removeById).toHaveBeenCalledWith('test-1');
    expect(hostComponent.deletedId).toBeNull();
  });
});
