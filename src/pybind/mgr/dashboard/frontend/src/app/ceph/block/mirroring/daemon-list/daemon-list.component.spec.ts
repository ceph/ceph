import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { DaemonListComponent } from './daemon-list.component';

describe('DaemonListComponent', () => {
  let component: DaemonListComponent;
  let fixture: ComponentFixture<DaemonListComponent>;

  configureTestBed({
    declarations: [DaemonListComponent, MirrorHealthColorPipe],
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ProgressbarModule.forRoot(),
      HttpClientTestingModule
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DaemonListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
