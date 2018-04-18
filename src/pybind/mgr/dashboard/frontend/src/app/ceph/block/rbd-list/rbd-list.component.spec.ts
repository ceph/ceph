import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import {
  AlertModule,
  BsDropdownModule,
  ModalModule,
  TabsModule,
  TooltipModule
} from 'ngx-bootstrap';

import { ComponentsModule } from '../../../shared/components/components.module';
import { SharedModule } from '../../../shared/shared.module';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdListComponent } from './rbd-list.component';

describe('RbdListComponent', () => {
  let component: RbdListComponent;
  let fixture: ComponentFixture<RbdListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        BsDropdownModule.forRoot(),
        TabsModule.forRoot(),
        ModalModule.forRoot(),
        TooltipModule.forRoot(),
        ToastModule.forRoot(),
        AlertModule.forRoot(),
        ComponentsModule,
        RouterTestingModule,
        HttpClientTestingModule
      ],
      declarations: [ RbdListComponent, RbdDetailsComponent, RbdSnapshotListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
