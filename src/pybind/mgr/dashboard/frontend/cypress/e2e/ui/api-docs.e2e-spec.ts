import { ApiDocsPageHelper } from '../ui/api-docs.po';

describe('Api Docs Page', () => {
  const apiDocs = new ApiDocsPageHelper();

  beforeEach(() => {
    cy.login();
    apiDocs.navigateTo();
  });

  it('should show the API Docs description', () => {
    cy.get('.renderedMarkdown').first().contains('This is the official Ceph REST API');
  });
});
