import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Router } from '@angular/router';
import { of } from 'rxjs';
import { ToastContent } from 'carbon-components-angular';

import { ToastComponent } from './notification-toast.component';
import { NotificationService } from '../../services/notification.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('ToastComponent', () => {
  let component: ToastComponent;
  let fixture: ComponentFixture<ToastComponent>;
  let mockToasts: ToastContent[];
  let mockRouter: any;

  const mockNotificationService = {
    activeToasts$: of([]),
    removeToast: jest.fn(),
    clearAllToasts: jest.fn()
  };

  mockRouter = {
    navigateByUrl: jest.fn()
  };

  configureTestBed({
    declarations: [ToastComponent],
    imports: [NoopAnimationsModule],
    providers: [
      {
        provide: NotificationService,
        useValue: mockNotificationService
      },
      {
        provide: Router,
        useValue: mockRouter
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

  describe('view more click', () => {
    it('should navigate and clear toasts when view-more link is clicked', () => {
      fixture.detectChanges();
      const link = document.createElement('a');
      link.classList.add('toast-view-more');
      link.setAttribute('href', '#/notifications?id=abc123');
      fixture.nativeElement.appendChild(link);

      const event = new MouseEvent('click', { bubbles: true });
      const preventDefaultSpy = jest.spyOn(event, 'preventDefault');
      link.dispatchEvent(event);

      expect(preventDefaultSpy).toHaveBeenCalled();
      expect(mockRouter.navigateByUrl).toHaveBeenCalledWith('/notifications?id=abc123');
      expect(mockNotificationService.clearAllToasts).toHaveBeenCalled();
    });

    it('should not navigate for non view-more clicks', () => {
      fixture.detectChanges();
      mockRouter.navigateByUrl.mockClear();
      mockNotificationService.clearAllToasts.mockClear();

      const span = document.createElement('span');
      fixture.nativeElement.appendChild(span);
      span.dispatchEvent(new MouseEvent('click', { bubbles: true }));

      expect(mockRouter.navigateByUrl).not.toHaveBeenCalled();
      expect(mockNotificationService.clearAllToasts).not.toHaveBeenCalled();
    });
  });
});
