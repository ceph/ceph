import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { DaemonListComponent } from '../daemon-list/daemon-list.component';
import { ImageListComponent } from '../image-list/image-list.component';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { PoolListComponent } from '../pool-list/pool-list.component';
import { OverviewComponent } from './overview.component';

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  configureTestBed({
    declarations: [
      DaemonListComponent,
      ImageListComponent,
      MirrorHealthColorPipe,
      OverviewComponent,
      PoolListComponent
    ],
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ProgressbarModule.forRoot(),
      HttpClientTestingModule,
      RouterTestingModule,
      ToastModule.forRoot()
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
