import { ComponentFixture, TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NfsClusterComponent } from './nfs-cluster.component';

describe('NfsClusterComponent', () => {
  let component: NfsClusterComponent;
  let fixture: ComponentFixture<NfsClusterComponent>;

  configureTestBed({
    declarations: [NfsClusterComponent],
    imports: [HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterComponent);
    component = fixture.componentInstance;
  });
  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
