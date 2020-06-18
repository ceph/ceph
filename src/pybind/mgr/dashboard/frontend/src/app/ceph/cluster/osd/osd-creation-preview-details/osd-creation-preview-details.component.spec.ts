import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { OsdCreationPreviewDetailsComponent } from './osd-creation-preview-details.component';

describe('OsdCreationPreviewDetailsComponent', () => {
  let component: OsdCreationPreviewDetailsComponent;
  let fixture: ComponentFixture<OsdCreationPreviewDetailsComponent>;

  configureTestBed({
    imports: [TabsModule.forRoot()],
    declarations: [OsdCreationPreviewDetailsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdCreationPreviewDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
