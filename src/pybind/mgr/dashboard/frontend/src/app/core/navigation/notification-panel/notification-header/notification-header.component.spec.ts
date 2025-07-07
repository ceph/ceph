import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NotificationHeaderComponent } from './notification-header.component';
import { NotificationService } from '../../../../shared/services/notification.service';
import { BehaviorSubject } from 'rxjs';

describe('NotificationHeaderComponent', () => {
  let component: NotificationHeaderComponent;
  let fixture: ComponentFixture<NotificationHeaderComponent>;
  let notificationService: NotificationService;
  let muteStateSubject: BehaviorSubject<boolean>;

  beforeEach(async () => {
    muteStateSubject = new BehaviorSubject<boolean>(false);
    await TestBed.configureTestingModule({
      declarations: [NotificationHeaderComponent],
      providers: [
        {
          provide: NotificationService,
          useValue: {
            muteState$: muteStateSubject.asObservable(),
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
    expect(component.isMuted).toBe(false);
  });

  it('should update mute state when subscription emits', () => {
    muteStateSubject.next(true);
    fixture.detectChanges();
    expect(component.isMuted).toBe(true);
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
