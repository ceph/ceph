import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterListComponent } from './smb-cluster-list.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

describe('SmbClusterListComponent', () => {
  let component: SmbClusterListComponent;
  let fixture: ComponentFixture<SmbClusterListComponent>;

  configureTestBed({
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [SmbClusterListComponent]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(SmbClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
