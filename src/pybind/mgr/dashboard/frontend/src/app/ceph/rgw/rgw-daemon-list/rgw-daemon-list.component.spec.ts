import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { PerformanceCounterModule } from '../../performance-counter/performance-counter.module';
import { RgwDaemonDetailsComponent } from '../rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list.component';

describe('RgwDaemonListComponent', () => {
  let component: RgwDaemonListComponent;
  let fixture: ComponentFixture<RgwDaemonListComponent>;

  configureTestBed({
    declarations: [RgwDaemonListComponent, RgwDaemonDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      TabsModule.forRoot(),
      PerformanceCounterModule,
      SharedModule,
      RouterTestingModule
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
