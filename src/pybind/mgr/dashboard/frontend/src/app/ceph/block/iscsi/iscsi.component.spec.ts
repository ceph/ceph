import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { TcmuIscsiService } from '../../../shared/api/tcmu-iscsi.service';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { ListPipe } from '../../../shared/pipes/list.pipe';
import { RelativeDatePipe } from '../../../shared/pipes/relative-date.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { IscsiComponent } from './iscsi.component';

describe('IscsiComponent', () => {
  let component: IscsiComponent;
  let fixture: ComponentFixture<IscsiComponent>;

  const fakeService = {
    tcmuiscsi: () => {
      return new Promise(function(resolve, reject) {
        return;
      });
    }
  };

  configureTestBed({
    imports: [],
    declarations: [IscsiComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      CephShortVersionPipe,
      DimlessPipe,
      FormatterService,
      RelativeDatePipe,
      ListPipe,
      { provide: TcmuIscsiService, useValue: fakeService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
