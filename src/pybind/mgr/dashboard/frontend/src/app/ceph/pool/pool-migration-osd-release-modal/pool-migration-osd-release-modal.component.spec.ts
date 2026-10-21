import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';
import { LinkModule, ModalModule } from 'carbon-components-angular';

import { PoolMigrationOsdReleaseModalComponent } from './pool-migration-osd-release-modal.component';

describe('PoolMigrationOsdReleaseModalComponent', () => {
  let component: PoolMigrationOsdReleaseModalComponent;
  let fixture: ComponentFixture<PoolMigrationOsdReleaseModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        ModalModule,
        LinkModule,
        RouterTestingModule.withRoutes([
          { path: 'upgrade', component: PoolMigrationOsdReleaseModalComponent }
        ])
      ],
      declarations: [PoolMigrationOsdReleaseModalComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(PoolMigrationOsdReleaseModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show the title and description', () => {
    const text = fixture.nativeElement.textContent;
    expect(text).toContain('Cannot migrate pool');
    expect(text).toContain('not every OSD of this cluster is upgraded to');
  });

  it('should link to the upgrade page', () => {
    const link = fixture.debugElement.query(By.css('[data-testid="upgrade-page-link"]'));
    expect(link.nativeElement.getAttribute('href')).toBe('/upgrade');
  });

  it('should link to the upgrade documentation in a new tab', () => {
    const link = fixture.debugElement.query(By.css('[data-testid="documentation-link"]'));
    expect(link.nativeElement.getAttribute('href')).toBe(
      'https://docs.ceph.com/en/latest/cephadm/upgrade/#upgrading-ceph'
    );
    expect(link.nativeElement.getAttribute('target')).toBe('_blank');
    expect(link.nativeElement.getAttribute('rel')).toContain('noopener');
  });

  it('should close when the upgrade link is clicked', () => {
    const closeSpy = jest.spyOn(component, 'closeModal');
    fixture.debugElement.query(By.css('[data-testid="upgrade-page-link"]')).nativeElement.click();
    expect(closeSpy).toHaveBeenCalled();
  });
});
