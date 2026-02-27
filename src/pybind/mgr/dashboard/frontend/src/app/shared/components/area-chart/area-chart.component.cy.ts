import { AreaChartComponent } from './area-chart.component';
import { ChartsModule } from '@carbon/charts-angular';
import { CUSTOM_ELEMENTS_SCHEMA, EventEmitter } from '@angular/core';
import { NumberFormatterService } from '@ceph-dashboard/shared';

describe('AreaChartComponent', () => {
  const mockData = [
    {
      timestamp: new Date('2024-01-01T00:00:00Z'),
      values: { read: 1024, write: 2048 }
    },
    {
      timestamp: new Date('2024-01-01T00:01:00Z'),
      values: { read: 1536, write: 4096 }
    }
  ];

  it('should mount', () => {
    cy.mount(AreaChartComponent, {
      imports: [ChartsModule],
      providers: [NumberFormatterService],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    });
  });

  it('should render chart data and emit formatted values', () => {
    const emitSpy = cy.spy().as('currentFormattedValuesSpy');

    const currentFormattedValues = new EventEmitter<{
      key: string;
      values: Record<string, string>;
    }>();
    currentFormattedValues.emit = emitSpy;

    cy.mount(AreaChartComponent, {
      componentProperties: {
        chartTitle: 'Test Chart',
        dataUnit: 'B/s',
        chartKey: 'test-key',
        rawData: mockData,
        currentFormattedValues
      },
      imports: [ChartsModule],
      providers: [NumberFormatterService],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    });

    cy.get('@currentFormattedValuesSpy').should('have.been.calledOnce');
    cy.contains('Test Chart').should('exist');
  });

  it('should set correct chartOptions based on max value', () => {
    cy.mount(AreaChartComponent, {
      componentProperties: {
        chartTitle: 'Test Chart',
        dataUnit: 'B/s',
        rawData: mockData,
        chartKey: 'test-key'
      },
      imports: [ChartsModule],
      providers: [NumberFormatterService],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).then(({ component }) => {
      const options = component.chartOptions;
      expect(options?.axes?.left?.domain?.[1]).to.be.greaterThan(0);
      expect(options?.tooltip?.enabled).to.equal(true);
      expect(options?.axes?.bottom?.scaleType).to.equal('time');
    });
  });
});
