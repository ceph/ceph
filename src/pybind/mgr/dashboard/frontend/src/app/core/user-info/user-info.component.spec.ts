import { Location } from '@angular/common';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { Observable } from 'rxjs/Observable';

import { AuthService } from '../../shared/api/auth.service';
import { UserService } from '../../shared/api/user.service';
import { AuthStorageService } from '../../shared/services/auth-storage.service';
import { NotificationService } from '../../shared/services/notification.service';
import { SharedModule } from '../../shared/shared.module';
import { UserInfoComponent } from './user-info.component';

describe('UserInfoComponent', () => {
  let component: UserInfoComponent;
  let fixture: ComponentFixture<UserInfoComponent>;

  const fakeService = {
    get: (username: string) => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [UserInfoComponent],
        imports: [FormsModule, ReactiveFormsModule, SharedModule, RouterTestingModule],
        providers: [
          Location,
          { provide: UserService, useValue: fakeService },
          { provide: AuthService, useValue: fakeService },
          { provide: NotificationService, useValue: fakeService }
        ]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserInfoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
