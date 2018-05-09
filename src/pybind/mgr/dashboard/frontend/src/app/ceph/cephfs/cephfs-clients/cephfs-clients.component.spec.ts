import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsClientsComponent } from './cephfs-clients.component';

describe('CephfsClientsComponent', () => {
  let component: CephfsClientsComponent;
  let fixture: ComponentFixture<CephfsClientsComponent>;

  const fakeFilesystemService = {
    getCephfs: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    },
    getClients: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule, BsDropdownModule.forRoot(), SharedModule],
      declarations: [CephfsClientsComponent],
      providers: [{ provide: CephfsService, useValue: fakeFilesystemService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsClientsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
