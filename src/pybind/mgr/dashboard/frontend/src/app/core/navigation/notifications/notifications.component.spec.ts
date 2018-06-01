import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopoverModule } from 'ngx-bootstrap/popover';

import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;

  const fakeService = new NotificationService(null, null);

  configureTestBed({
    imports: [PopoverModule.forRoot(), SharedModule],
    declarations: [NotificationsComponent],
    providers: [{ provide: NotificationService, useValue: fakeService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
