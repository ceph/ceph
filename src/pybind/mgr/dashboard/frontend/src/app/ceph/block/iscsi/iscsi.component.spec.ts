import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { IscsiService } from '../../../shared/api/iscsi.service';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { IscsiBackstorePipe } from '../../../shared/pipes/iscsi-backstore.pipe';
import { RelativeDatePipe } from '../../../shared/pipes/relative-date.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiComponent } from './iscsi.component';

describe('IscsiComponent', () => {
  let component: IscsiComponent;
  let fixture: ComponentFixture<IscsiComponent>;
  let iscsiService: IscsiService;
  let tcmuiscsiData: Record<string, any>;

  const fakeService = {
    overview: () => {
      return new Promise(function () {
        return;
      });
    }
  };

  configureTestBed({
    imports: [BrowserAnimationsModule, SharedModule],
    declarations: [IscsiComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      CephShortVersionPipe,
      DimlessPipe,
      FormatterService,
      RelativeDatePipe,
      IscsiBackstorePipe,
      { provide: IscsiService, useValue: fakeService },
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiComponent);
    component = fixture.componentInstance;
    iscsiService = TestBed.inject(IscsiService);
    fixture.detectChanges();
    tcmuiscsiData = {
      images: []
    };
    spyOn(iscsiService, 'overview').and.callFake(() => of(tcmuiscsiData));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should refresh without stats available', () => {
    tcmuiscsiData.images.push({});
    component.refresh();
    expect(component.images[0].cdIsBinary).toBe(true);
  });

  it('should refresh with stats', () => {
    tcmuiscsiData.images.push({
      stats_history: {
        rd_bytes: [
          [1540551220, 0.0],
          [1540551225, 0.0],
          [1540551230, 0.0]
        ],
        wr_bytes: [
          [1540551220, 0.0],
          [1540551225, 0.0],
          [1540551230, 0.0]
        ]
      }
    });
    component.refresh();
    expect(component.images[0].stats_history).toEqual({ rd_bytes: [0, 0, 0], wr_bytes: [0, 0, 0] });
    expect(component.images[0].cdIsBinary).toBe(true);
  });
});
