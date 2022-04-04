import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgbNavModule, NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';

import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { ImageListComponent } from './image-list.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('ImageListComponent', () => {
  let component: ImageListComponent;
  let fixture: ComponentFixture<ImageListComponent>;

  configureTestBed({
    declarations: [ImageListComponent, MirrorHealthColorPipe],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      NgbNavModule,
      NgbProgressbarModule,
      HttpClientTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ImageListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
