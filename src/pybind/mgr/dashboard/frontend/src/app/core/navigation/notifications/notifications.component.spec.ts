import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastModule } from 'ng2-toastr';
import { PopoverModule } from 'ngx-bootstrap/popover';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;

  configureTestBed({
    imports: [PopoverModule.forRoot(), SharedModule, ToastModule.forRoot()],
    declarations: [NotificationsComponent],
    providers: [NotificationService]
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
