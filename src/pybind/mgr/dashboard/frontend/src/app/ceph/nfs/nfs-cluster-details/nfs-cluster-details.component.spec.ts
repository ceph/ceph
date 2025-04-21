import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NfsClusterDetailsComponent } from './nfs-cluster-details.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('NfsClusterDetailsComponent', () => {
  let component: NfsClusterDetailsComponent;
  let fixture: ComponentFixture<NfsClusterDetailsComponent>;

  configureTestBed({
    declarations: [NfsClusterDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterDetailsComponent);
    component = fixture.componentInstance;
  });
  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
