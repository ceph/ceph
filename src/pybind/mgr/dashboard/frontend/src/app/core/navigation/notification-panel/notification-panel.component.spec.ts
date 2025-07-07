import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NotificationPanelComponent } from './notification-panel.component';
import { NotificationService } from '../../../shared/services/notification.service';

describe('NotificationPanelComponent', () => {
  let component: NotificationPanelComponent;
  let fixture: ComponentFixture<NotificationPanelComponent>;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NotificationPanelComponent],
      providers: [
        {
          provide: NotificationService,
          useValue: {
            toggleSidebar: jasmine.createSpy('toggleSidebar')
          }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationPanelComponent);
    component = fixture.componentInstance;
    notificationService = TestBed.inject(NotificationService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('handleClickOutside', () => {
    it('should close sidebar when clicking outside', () => {
      // Create a click event outside the component
      const outsideClickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true
      });
      document.dispatchEvent(outsideClickEvent);

      expect(notificationService.toggleSidebar).toHaveBeenCalledWith(false, true);
    });

    it('should not close sidebar when clicking inside', () => {
      // Create a click event inside the component
      const insideClickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true
      });

      const componentElement = fixture.nativeElement;
      componentElement.dispatchEvent(insideClickEvent);

      expect(notificationService.toggleSidebar).not.toHaveBeenCalled();
    });
  });
});
