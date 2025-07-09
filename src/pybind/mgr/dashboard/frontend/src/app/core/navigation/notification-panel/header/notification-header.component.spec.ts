import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NotificationHeaderComponent } from './notification-header.component';
import { NotificationService } from '../../../../shared/services/notification.service';
import { of } from 'rxjs';

describe('NotificationHeaderComponent', () => {
  let component: NotificationHeaderComponent;
  let fixture: ComponentFixture<NotificationHeaderComponent>;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NotificationHeaderComponent],
      providers: [
        {
          provide: NotificationService,
          useValue: {
            muteState$: of(false),
            removeAll: jasmine.createSpy('removeAll'),
            suspendToasties: jasmine.createSpy('suspendToasties')
          }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NotificationHeaderComponent);
    component = fixture.componentInstance;
    notificationService = TestBed.inject(NotificationService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default mute state', () => {
    expect(component.isMuted).toBeFalse();
  });

  it('should update mute state when subscription emits', () => {
    const muteState$ = of(true);
    spyOnProperty(notificationService, 'muteState$').and.returnValue(muteState$);

    component.ngOnInit();
    expect(component.isMuted).toBeTrue();
  });

  it('should emit dismissAll event and call removeAll on dismiss', () => {
    spyOn(component.dismissAll, 'emit');

    component.onDismissAll();

    expect(component.dismissAll.emit).toHaveBeenCalled();
    expect(notificationService.removeAll).toHaveBeenCalled();
  });

  it('should toggle mute state', () => {
    component.isMuted = false;
    component.onToggleMute();
    expect(notificationService.suspendToasties).toHaveBeenCalledWith(true);

    component.isMuted = true;
    component.onToggleMute();
    expect(notificationService.suspendToasties).toHaveBeenCalledWith(false);
  });

  it('should unsubscribe on destroy', () => {
    spyOn(component['subs'], 'unsubscribe');
    component.ngOnDestroy();
    expect(component['subs'].unsubscribe).toHaveBeenCalled();
  });
}); 