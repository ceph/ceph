import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../../shared/shared.module';
import { RgwUserDetailsComponent } from '../rgw-user-details/rgw-user-details.component';
import { RgwUserListComponent } from './rgw-user-list.component';

describe('RgwUserListComponent', () => {
  let component: RgwUserListComponent;
  let fixture: ComponentFixture<RgwUserListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        RgwUserListComponent,
        RgwUserDetailsComponent
      ],
      imports: [
        HttpClientModule,
        BsDropdownModule.forRoot(),
        TabsModule.forRoot(),
        SharedModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
