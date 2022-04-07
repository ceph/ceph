import { e2e } from '@grafana/e2e';
import { Then, When } from 'cypress-cucumber-preprocessor/steps';
import 'cypress-iframe';

function getIframe() {
  cy.frameLoaded('#iframe');
  return cy.iframe();
}

Then('I should see the grafana panel {string}', (panels: string) => {
  getIframe().within(() => {
    for (const panel of panels.split(', ')) {
      cy.get('.grafana-app')
        .wait(100)
        .within(() => {
          e2e.components.Panels.Panel.title(panel).should('be.visible');
        });
    }
  });
});

When('I view the grafana panel {string}', (panels: string) => {
  getIframe().within(() => {
    for (const panel of panels.split(', ')) {
      cy.get('.grafana-app')
        .wait(100)
        .within(() => {
          e2e.components.Panels.Panel.title(panel).should('be.visible').click();
          e2e.components.Panels.Panel.headerItems('View').should('be.visible').click();
        });
    }
  });
});

Then('I should not see {string} in the panel {string}', (value: string, panels: string) => {
  getIframe().within(() => {
    for (const panel of panels.split(', ')) {
      cy.get('.grafana-app')
        .wait(100)
        .within(() => {
          cy.get(`[aria-label="${panel} panel"]`)
            .should('be.visible')
            .within(() => {
              cy.get('span').first().should('not.have.text', value);
            });
        });
    }
  });
});

Then(
  'I should see the legends {string} in the graph {string}',
  (legends: string, panels: string) => {
    getIframe().within(() => {
      for (const panel of panels.split(', ')) {
        cy.get('.grafana-app')
          .wait(100)
          .within(() => {
            cy.get(`[aria-label="${panel} panel"]`)
              .should('be.visible')
              .within(() => {
                for (const legend of legends.split(', ')) {
                  cy.get('a').contains(legend);
                }
              });
          });
      }
    });
  }
);

Then('I should not see No Data in the graph {string}', (panels: string) => {
  getIframe().within(() => {
    for (const panel of panels.split(', ')) {
      cy.get('.grafana-app')
        .wait(100)
        .within(() => {
          cy.get(`[aria-label="${panel} panel"]`)
            .should('be.visible')
            .within(() => {
              cy.get('div.datapoints-warning').should('not.exist');
            });
        });
    }
  });
});
