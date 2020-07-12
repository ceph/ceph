import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { PoolListComponent } from './pool-list.component';

describe('PoolListComponent', () => {
  let component: PoolListComponent;
  let fixture: ComponentFixture<PoolListComponent>;

  configureTestBed({
    declarations: [PoolListComponent, MirrorHealthColorPipe],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
