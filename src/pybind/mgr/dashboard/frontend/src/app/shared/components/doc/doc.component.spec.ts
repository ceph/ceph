import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephReleaseNamePipe } from '~/app/shared/pipes/ceph-release-name.pipe';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DocComponent } from './doc.component';

describe('DocComponent', () => {
  let component: DocComponent;
  let fixture: ComponentFixture<DocComponent>;

  configureTestBed({
    declarations: [DocComponent],
    imports: [HttpClientTestingModule],
    providers: [CephReleaseNamePipe]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DocComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
