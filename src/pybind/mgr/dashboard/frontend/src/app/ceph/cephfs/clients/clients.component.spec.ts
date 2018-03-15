import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { SharedModule } from '../../../shared/shared.module';
import { CephfsService } from '../cephfs.service';
import { ClientsComponent } from './clients.component';

describe('ClientsComponent', () => {
  let component: ClientsComponent;
  let fixture: ComponentFixture<ClientsComponent>;

  const fakeFilesystemService = {
    getCephfs: id => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    },
    getClients: id => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [
          RouterTestingModule,
          BsDropdownModule.forRoot(),
          SharedModule
        ],
        declarations: [ClientsComponent],
        providers: [{ provide: CephfsService, useValue: fakeFilesystemService }]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ClientsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
