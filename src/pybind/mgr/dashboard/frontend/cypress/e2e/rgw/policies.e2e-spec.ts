import { PoliciesPageHelper } from './policies.po';
import { AccountsPageHelper } from './accounts.po';

describe('RGW policies page', () => {
  const policies = new PoliciesPageHelper();
  const accounts = new AccountsPageHelper();
  const accountName = 'policies-test-account';
  const policyName = 'TestReadOnlyPolicy';
  const policyDocument = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": "*"
    }
  ]
}`;

  before(() => {
    cy.login();
    accounts.navigateTo();
    cy.get('body').then(($body) => {
      if (!$body.text().includes(accountName)) {
        accounts.navigateTo('create');
        accounts.create({ name: accountName, email: 'policies-test@example.com' });
      }
    });
  });

  after(() => {
    cy.login();
    accounts.navigateTo();
    cy.get('body').then(($body) => {
      if ($body.text().includes(accountName)) {
        accounts.delete(accountName, null, null, true, false, false, false);
      }
    });
  });

  beforeEach(() => {
    cy.login();
    accounts.navigateTo();
    accounts.getResourcePage(accountName).click();
    cy.contains('cds-sidenav-item a', /^Policies$/).click();
    cy.location('hash').should('include', '/policies');
    cy.get('cd-rgw-account-policies-list').should('exist');
  });

  describe('Create, View & Delete IAM policies', () => {
    it('should create IAM policy', () => {
      policies.create(policyName, '/', policyDocument);
      policies.checkExist(policyName, true);
    });

    it('should open IAM policy details', () => {
      policies.openPolicy(policyName);
    });

    it('should test policy versions and tags in details modal', () => {
      const versionDoc = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": "*"
    }
  ]
}`;
      policies.testVersions(policyName, versionDoc);
      policies.testTags(policyName, 'Environment', 'Testing');
    });

    it('should delete IAM policy', () => {
      policies.deletePolicy(policyName);
      policies.checkExist(policyName, false);
    });
  });
});
