import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsListComponent } from './smb-usersgroups-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

describe('SmbUsersgroupsListComponent', () => {
  let component: SmbUsersgroupsListComponent;
  let fixture: ComponentFixture<SmbUsersgroupsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbUsersgroupsListComponent],
      imports: [SharedModule, HttpClientTestingModule, ToastrModule.forRoot(), RouterTestingModule]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
