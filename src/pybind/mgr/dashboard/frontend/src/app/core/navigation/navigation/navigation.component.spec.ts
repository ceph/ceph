import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { CollapseModule, PopoverModule } from 'ngx-bootstrap';

import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { LogoutComponent } from '../../auth/logout/logout.component';
import { NotificationsComponent } from '../notifications/notifications.component';
import { TaskManagerComponent } from '../task-manager/task-manager.component';
import { NavigationComponent } from './navigation.component';

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;

  const fakeService = new NotificationService(null, null);

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        RouterTestingModule,
        HttpClientTestingModule,
        CollapseModule.forRoot(),
        PopoverModule.forRoot()
      ],
      declarations: [
        NavigationComponent,
        NotificationsComponent,
        LogoutComponent,
        TaskManagerComponent
      ],
      providers: [{ provide: NotificationService, useValue: fakeService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
