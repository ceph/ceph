import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { DaemonListComponent } from './daemon-list.component';

describe('DaemonListComponent', () => {
  let component: DaemonListComponent;
  let fixture: ComponentFixture<DaemonListComponent>;

  configureTestBed({
    declarations: [DaemonListComponent, MirrorHealthColorPipe],
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule]
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
