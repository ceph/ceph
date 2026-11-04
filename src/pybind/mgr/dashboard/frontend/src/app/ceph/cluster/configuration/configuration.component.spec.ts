import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ButtonModule } from 'carbon-components-angular';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ConfigurationComponent } from './configuration.component';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

describe('ConfigurationComponent', () => {
  let component: ConfigurationComponent;
  let fixture: ComponentFixture<ConfigurationComponent>;

  configureTestBed({
    declarations: [ConfigurationComponent, TableComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      FormsModule,
      NgbNavModule,
      ButtonModule,
      HttpClientTestingModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('collapsible value lists', () => {
    const row: any = { name: 'crush_location' };

    it('should be collapsed by default', () => {
      expect(component.isValueListExpanded(row)).toBe(false);
    });

    it('should toggle expanded state', () => {
      component.toggleValueList(row);
      expect(component.isValueListExpanded(row)).toBe(true);
      component.toggleValueList(row);
      expect(component.isValueListExpanded(row)).toBe(false);
    });

    it('should keep expanded state after the row object is replaced on refresh', () => {
      component.toggleValueList(row);
      expect(component.isValueListExpanded({ ...row })).toBe(true);
      expect(component.isValueListExpanded({ name: 'other_option' } as any)).toBe(false);
    });

    it('should not propagate the click to the table row', () => {
      const event = new MouseEvent('click');
      const spy = jest.spyOn(event, 'stopPropagation');
      component.toggleValueList(row, event);
      expect(spy).toHaveBeenCalled();
    });

    describe('template', () => {
      const renderValues = (count: number) => {
        const value = Array.from({ length: count }, (_, i) => ({
          section: `osd.${i}`,
          value: `host=node${i}`
        }));
        const host = document.createElement('div');
        const view = component.confValTpl.createEmbeddedView({
          data: { value, row: { name: 'crush_location' } }
        });
        view.detectChanges();
        view.rootNodes.forEach((n) => host.appendChild(n));
        return { host, view };
      };

      it('should render all values when the list is short', () => {
        const { host } = renderValues(component.collapsedValueLimit);
        expect(host.textContent).toContain(`osd.${component.collapsedValueLimit - 1}`);
        expect(host.querySelector('button')).toBeNull();
      });

      it('should render a preview and expand it on click', () => {
        const { host, view } = renderValues(444);
        const button = host.querySelector('button');
        expect(host.textContent).toContain(`osd.${component.collapsedValueLimit - 1}`);
        expect(host.textContent).not.toContain(`osd.${component.collapsedValueLimit}:`);
        expect(button.textContent).toContain(`${444 - component.collapsedValueLimit} more`);
        expect(button.getAttribute('aria-expanded')).toBe('false');

        button.click();
        view.detectChanges();
        expect(host.textContent).toContain('osd.443');
        expect(button.textContent).toContain('Show less');
        expect(button.getAttribute('aria-expanded')).toBe('true');
      });
    });

    describe('table refresh', () => {
      const setValueCount = (count: number) => {
        // Like getConfigurationList(): every refresh creates new row objects.
        component.data = [
          {
            name: 'crush_location',
            desc: '',
            level: 'basic',
            services: ['osd'],
            can_update_at_runtime: true,
            value: Array.from({ length: count }, (_, i) => ({
              section: `osd.${i}`,
              value: `host=node${i}`
            })),
            cdLink: '/configuration/crush_location'
          }
        ];
        fixture.detectChanges();
      };
      const valueCell = (): HTMLElement =>
        Array.from<HTMLElement>(fixture.nativeElement.querySelectorAll('tbody td')).find((td) =>
          td.textContent.includes('osd.0:')
        );
      const shownValues = () => valueCell().textContent.match(/osd\.\d+:/g).length;
      const toggleButton = (): HTMLButtonElement => valueCell().querySelector('button');

      beforeEach(() => {
        const cdTable: TableComponent = fixture.debugElement.query(
          By.directive(TableComponent)
        ).componentInstance;
        cdTable.ngAfterViewInit();
      });

      it('should show or hide the toggle when the number of values changes', () => {
        setValueCount(3);
        expect(shownValues()).toBe(3);
        expect(toggleButton()).toBeNull();

        setValueCount(8);
        expect(shownValues()).toBe(component.collapsedValueLimit);
        expect(toggleButton().textContent).toContain(`${8 - component.collapsedValueLimit} more`);

        setValueCount(4);
        expect(shownValues()).toBe(4);
        expect(toggleButton()).toBeNull();
      });

      it('should keep an expanded list expanded across refreshes', () => {
        setValueCount(8);
        toggleButton().click();
        fixture.detectChanges();
        expect(shownValues()).toBe(8);

        setValueCount(12);
        expect(shownValues()).toBe(12);
        expect(toggleButton().textContent).toContain('Show less');

        // Dropping below the limit hides the toggle but the option stays expanded.
        setValueCount(4);
        expect(toggleButton()).toBeNull();
        setValueCount(9);
        expect(shownValues()).toBe(9);
        expect(toggleButton().textContent).toContain('Show less');
      });
    });
  });

  // TODO: Re-write this unit test to reflect latest changes on datatble markup
  it.skip('should check header text', () => {
    const cdTableEl = fixture.debugElement.query(By.directive(TableComponent));
    const cdTableComponent: TableComponent = cdTableEl.componentInstance;
    cdTableComponent.ngAfterViewInit();
    fixture.detectChanges();
    const actual = fixture.debugElement.query(By.css('thead')).nativeElement.textContent.trim();
    const expected = 'Name  Description  Current value  Default  Editable';
    expect(actual).toBe(expected);
  });
});
