import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthListComponent } from './smb-join-auth-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';

describe('SmbJoinAuthListComponent', () => {
  let component: SmbJoinAuthListComponent;
  let fixture: ComponentFixture<SmbJoinAuthListComponent>;

  configureTestBed({
    declarations: [SmbJoinAuthListComponent],
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(SmbJoinAuthListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
