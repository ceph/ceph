import { beforeEach, describe, expect, it } from '@jest/globals';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { OverviewComponent } from './resource-overview-card.component';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [OverviewComponent],
      imports: [PipesModule],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Template Rendering', () => {
    it('should display the title if provided', () => {
      component.title = 'Host Overview';
      fixture.detectChanges();

      const titleEl = fixture.debugElement.query(By.css('.cds--type-heading-03'));
      expect(titleEl).toBeTruthy();
      expect(titleEl.nativeElement.textContent.trim()).toBe('Host Overview');
    });

    it('should render standard text values correctly', () => {
      component.fields = [{ label: 'CPU', value: 'Intel', type: 'text' }];
      fixture.detectChanges();

      const labelEl = fixture.debugElement.query(By.css('.cd-overview-label'));
      const valueEl = fixture.debugElement.query(By.css('.cd-overview-value'));

      expect(labelEl.nativeElement.textContent.trim()).toBe('CPU');
      expect(valueEl.nativeElement.textContent.trim()).toBe('Intel');
    });

    it('should render tags correctly', () => {
      component.fields = [
        {
          label: 'Roles',
          values: ['mon', 'mgr', 'osd'],
          type: 'tags'
        }
      ];
      fixture.detectChanges();

      const tags = fixture.debugElement.queryAll(By.css('cds-tag'));
      expect(tags.length).toBe(3);
      expect(tags[0].nativeElement.textContent.trim()).toBe('mon');
      expect(tags[1].nativeElement.textContent.trim()).toBe('mgr');
      expect(tags[2].nativeElement.textContent.trim()).toBe('osd');
    });

    it('should render fallback emptyText for tags if values array is empty', () => {
      component.fields = [
        {
          label: 'Roles',
          values: [],
          type: 'tags',
          emptyText: 'No roles assigned'
        }
      ];
      fixture.detectChanges();

      const emptyTextEl = fixture.debugElement.query(By.css('.cd-overview-tags span'));
      expect(emptyTextEl.nativeElement.textContent.trim()).toBe('No roles assigned');
    });

    it('should render status fields as plain text values', () => {
      component.fields = [
        { label: 'State1', value: 'Available', type: 'status', status: 'success' },
        { label: 'State2', value: 'Maintenance', type: 'status', status: 'warning' },
        { label: 'State3', value: 'Custom State', type: 'status', status: 'info-circle' }
      ];
      fixture.detectChanges();

      const statusContainers = fixture.debugElement.queryAll(By.css('.cd-overview-status'));
      expect(statusContainers.length).toBe(3);

      expect(statusContainers[0].nativeElement.textContent.trim()).toContain('Available');
      expect(statusContainers[1].nativeElement.textContent.trim()).toContain('Maintenance');
      expect(statusContainers[2].nativeElement.textContent.trim()).toContain('Custom State');
    });

    it('should apply fallback info status class for info statuses', () => {
      component.fields = [
        {
          label: 'State',
          value: 'Custom State',
          type: 'status',
          status: 'info-circle'
        }
      ];
      fixture.detectChanges();

      const statusText = fixture.debugElement.query(
        By.css('.cd-overview-status .cd-status-text--info')
      );
      expect(statusText).toBeTruthy();
    });
  });

  describe('Loading State', () => {
    it('should set loading property to true', () => {
      component.loading = true;
      fixture.detectChanges();

      expect(component.loading).toBe(true);
    });

    it('should calculate the correct number of loading placeholders based on columns when fields are empty', () => {
      component.columns = 4;
      component.fields = [];
      component.ngOnChanges();

      expect(component.loadingPlaceholders.length).toBe(4);
      expect(component.loadingPlaceholders).toEqual([0, 1, 2, 3]);
    });

    it('should calculate the correct number of loading placeholders based on fields length when fields exist', () => {
      component.columns = 4;
      component.fields = [
        { label: 'Field 1', value: '1' },
        { label: 'Field 2', value: '2' }
      ];
      component.ngOnChanges();

      // Should prefer fields.length (2) over columns (4)
      expect(component.loadingPlaceholders.length).toBe(2);
      expect(component.loadingPlaceholders).toEqual([0, 1]);
    });

    it('should always render at least 1 placeholder if both fields length and columns are 0', () => {
      component.columns = 0;
      component.fields = [];
      component.ngOnChanges();

      expect(component.loadingPlaceholders.length).toBe(1);
    });
  });
});
