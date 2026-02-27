import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';
import { ToastContent } from 'carbon-components-angular';

import { ToastComponent } from './notification-toast.component';
import { NotificationService } from '../../services/notification.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('ToastComponent', () => {
  let component: ToastComponent;
  let fixture: ComponentFixture<ToastComponent>;
  let mockToasts: ToastContent[];

  const mockNotificationService = {
    activeToasts$: of([]),
    removeToast: jest.fn()
  };

  configureTestBed({
    declarations: [ToastComponent],
    imports: [NoopAnimationsModule],
    providers: [
      {
        provide: NotificationService,
        useValue: mockNotificationService
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ToastComponent);
    component = fixture.componentInstance;

    mockToasts = [
      {
        type: 'success',
        title: 'Test Title',
        subtitle: 'Test Message',
        caption: 'Test Caption',
        lowContrast: false,
        showClose: true
      }
    ];
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should initialize with activeToasts$ Observable', () => {
    fixture.detectChanges();
    expect(component.activeToasts$).toBeDefined();
  });

  it('should update activeToasts$ when notification service emits new toasts', () => {
    mockNotificationService.activeToasts$ = of(mockToasts);
    fixture.detectChanges();

    component.activeToasts$.subscribe((toasts) => {
      expect(toasts).toEqual(mockToasts);
    });
  });

  it('should call removeToast when onToastClose is called', () => {
    const toast = mockToasts[0];
    component.onToastClose(toast);
    expect(mockNotificationService.removeToast).toHaveBeenCalledWith(toast);
  });
});
