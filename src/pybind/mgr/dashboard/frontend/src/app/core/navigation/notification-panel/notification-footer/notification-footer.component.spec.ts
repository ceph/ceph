import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NotificationFooterComponent } from './notification-footer.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

class MockNotificationService {
  toggleSidebar = () => {};
}

describe('NotificationFooterComponent', () => {
  let component: NotificationFooterComponent;
  let fixture: ComponentFixture<NotificationFooterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NotificationFooterComponent],
      providers: [{ provide: NotificationService, useClass: MockNotificationService }],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationFooterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render view all button', () => {
    const compiled = fixture.nativeElement as HTMLElement;
    const button = compiled.querySelector('cds-button');
    expect(button?.textContent).toContain('View all');
  });
});
