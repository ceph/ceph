import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { Observable } from 'rxjs/Observable';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { CephfsComponent } from './cephfs.component';

describe('CephfsComponent', () => {
  let component: CephfsComponent;
  let fixture: ComponentFixture<CephfsComponent>;

  const fakeFilesystemService = {
    getCephfs: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    },
    getMdsCounters: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule],
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [CephfsComponent, DimlessPipe],
      providers: [
        DimlessPipe,
        DimlessBinaryPipe,
        FormatterService,
        { provide: CephfsService, useValue: fakeFilesystemService }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
