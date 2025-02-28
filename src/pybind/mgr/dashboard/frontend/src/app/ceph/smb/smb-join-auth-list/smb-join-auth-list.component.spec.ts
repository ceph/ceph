import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthListComponent } from './smb-join-auth-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

describe('SmbJoinAuthListComponent', () => {
  let component: SmbJoinAuthListComponent;
  let fixture: ComponentFixture<SmbJoinAuthListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbJoinAuthListComponent],
      imports: [SharedModule, HttpClientTestingModule, ToastrModule.forRoot(), RouterTestingModule]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbJoinAuthListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
