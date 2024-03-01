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
          cy.get(`[aria-label="${panel} panel"]`).should('be.visible');
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
          cy.get(`[aria-label="${panel} panel"]`).within(() => {
            cy.get('h2').click();
          });
          cy.get('[aria-label="Panel header item View"]').click();
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
                  cy.get(`button`).contains(legend);
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
