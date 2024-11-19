import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterListComponent } from './smb-cluster-list.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

describe('SmbClusterListComponent', () => {
  let component: SmbClusterListComponent;
  let fixture: ComponentFixture<SmbClusterListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        RouterTestingModule
      ],
      declarations: [SmbClusterListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
