import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsDropdownModule, TabsModule } from 'ngx-bootstrap';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { MirroringComponent } from './mirroring.component';

describe('MirroringComponent', () => {
  let component: MirroringComponent;
  let fixture: ComponentFixture<MirroringComponent>;

  configureTestBed({
    declarations: [MirroringComponent, MirrorHealthColorPipe],
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ProgressbarModule.forRoot(),
      HttpClientTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MirroringComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
