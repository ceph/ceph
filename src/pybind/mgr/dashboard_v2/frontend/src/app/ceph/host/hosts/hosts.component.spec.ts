import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HostsComponent } from './hosts.component';
import { ServiceListPipe } from '../service-list.pipe';
import { SharedModule } from '../../../shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('HostsComponent', () => {
  let component: HostsComponent;
  let fixture: ComponentFixture<HostsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        HttpClientTestingModule
      ],
      declarations: [
        HostsComponent,
        ServiceListPipe
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HostsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
