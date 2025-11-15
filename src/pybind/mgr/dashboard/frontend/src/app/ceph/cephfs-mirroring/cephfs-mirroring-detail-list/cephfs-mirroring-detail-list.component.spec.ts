import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsMirroringDetailListComponent } from './cephfs-mirroring-detail-list.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

describe('CephfsMirroringListComponent', () => {
  let component: CephfsMirroringDetailListComponent;
  let fixture: ComponentFixture<CephfsMirroringDetailListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        RouterTestingModule
      ],
      declarations: [CephfsMirroringDetailListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringDetailListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have table columns', () => {
    expect(component.columns.length).toBeGreaterThan(0);
    const columnNames = component.columns.map((c) => c.name);
    expect(columnNames).toContain('Snapshot / Directory');
    expect(columnNames).toContain('State');
    expect(columnNames).toContain('Last Snap');
    expect(columnNames).toContain('Duration');
    expect(columnNames).toContain('Progress');
  });
});
