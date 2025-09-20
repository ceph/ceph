import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbOverviewComponent } from './smb-overview.component';
import { SharedModule } from '~/app/shared/shared.module';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';

describe('SmbOverviewComponent', () => {
  let component: SmbOverviewComponent;
  let fixture: ComponentFixture<SmbOverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SharedModule],
      declarations: [SmbOverviewComponent],
      providers: [provideHttpClient(), provideHttpClientTesting()]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
