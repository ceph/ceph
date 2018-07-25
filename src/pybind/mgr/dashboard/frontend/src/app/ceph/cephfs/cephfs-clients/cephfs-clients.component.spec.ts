import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CephfsService } from '../../../shared/api/cephfs.service';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsClientsComponent } from './cephfs-clients.component';

describe('CephfsClientsComponent', () => {
  let component: CephfsClientsComponent;
  let fixture: ComponentFixture<CephfsClientsComponent>;

  configureTestBed({
    imports: [
      RouterTestingModule,
      BsDropdownModule.forRoot(),
      SharedModule,
      HttpClientTestingModule
    ],
    declarations: [CephfsClientsComponent],
    providers: [CephfsService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsClientsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
