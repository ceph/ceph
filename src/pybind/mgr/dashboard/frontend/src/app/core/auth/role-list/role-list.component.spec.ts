import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RoleDetailsComponent } from '../role-details/role-details.component';
import { UserTabsComponent } from '../user-tabs/user-tabs.component';
import { RoleListComponent } from './role-list.component';

describe('RoleListComponent', () => {
  let component: RoleListComponent;
  let fixture: ComponentFixture<RoleListComponent>;

  configureTestBed({
    declarations: [RoleListComponent, RoleDetailsComponent, UserTabsComponent],
    imports: [
      SharedModule,
      ToastModule.forRoot(),
      TabsModule.forRoot(),
      RouterTestingModule,
      HttpClientTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RoleListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
