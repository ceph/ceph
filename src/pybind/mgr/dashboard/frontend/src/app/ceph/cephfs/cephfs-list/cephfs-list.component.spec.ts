import { Component, Input } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Observable } from 'rxjs/Observable';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsListComponent } from './cephfs-list.component';

@Component({ selector: 'cd-cephfs-detail', template: '' })
class CephfsDetailStubComponent {
  @Input() selection: CdTableSelection;
}

describe('CephfsListComponent', () => {
  let component: CephfsListComponent;
  let fixture: ComponentFixture<CephfsListComponent>;

  const fakeService = {
    get: (service_type: string, service_id: string) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [SharedModule],
      declarations: [CephfsListComponent, CephfsDetailStubComponent],
      providers: [{ provide: CephfsService, useValue: fakeService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
