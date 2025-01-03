import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { of } from 'rxjs';

import { IscsiService } from '~/app/shared/api/iscsi.service';
import { CephShortVersionPipe } from '~/app/shared/pipes/ceph-short-version.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { IscsiBackstorePipe } from '~/app/shared/pipes/iscsi-backstore.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
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
      IscsiBackstorePipe,
      { provide: IscsiService, useValue: fakeService }
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
