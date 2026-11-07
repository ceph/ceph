import { RolesPageHelper } from './roles.po';

describe('RGW roles page', () => {
  const roles = new RolesPageHelper();
  let accountName: string;
  let roleName: string;
  // Minimal valid IAM trust policy (empty "{}" is rejected by RGW role create).
  const trustPolicy =
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam:::user/testuser"},"Action":["sts:AssumeRole"]}]}';

  before(() => {
    // Generate inside before() so browserify/cache cannot freeze Date.now() at bundle time.
    accountName = `roles-test-acc-${Date.now()}-${Cypress._.random(0, 1e6)}`;
    roleName = `testRole-${Date.now()}-${Cypress._.random(0, 1e6)}`;
    // RGW enforces unique emails across accounts (409 AccountAlreadyExists).
    const accountEmail = `${accountName}@example.com`;

    cy.login();
    // Create account via API (same pattern as users.e2e-spec) so roles tests
    // are not blocked by accounts-form async validators / leftover UI state.
    cy.request({
      method: 'POST',
      url: 'api/rgw/accounts',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' },
      body: {
        account_name: accountName,
        email: accountEmail,
        max_users: 1000,
        max_buckets: 1000,
        max_roles: 1000,
        max_group: 1000,
        max_access_keys: 4
      }
    }).then((resp) => {
      expect(resp.status).to.be.oneOf([200, 201]);
      Cypress.env('roles_test_account_id', resp.body.id);
    });
  });

  after(() => {
    const accountId = Cypress.env('roles_test_account_id');
    if (!accountId) {
      return;
    }
    // Best-effort cleanup; do not fail the suite if the dev server flaps.
    cy.login();
    cy.request({
      method: 'DELETE',
      url: `api/rgw/accounts/${accountId}`,
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' },
      failOnStatusCode: false,
      timeout: 15000
    });
  });

  beforeEach(() => {
    // Ignore transient ng-serve HMR / lazy-chunk reload races during local runs.
    cy.on('uncaught:exception', (err) => {
      if (/Failed to fetch dynamically imported module|ChunkLoadError/i.test(err.message)) {
        return false;
      }
      return true;
    });

    cy.login();
    // Go straight to the account roles page (avoids flaky accounts-list render).
    cy.visit(`#/rgw/accounts/${encodeURIComponent(accountName)}/roles`);
    cy.location('hash').should('include', '/roles');
    cy.get('cd-rgw-account-roles-list').should('exist');
  });

  describe('Create, Edit & Delete rgw roles', () => {
    it('should create rgw role', () => {
      roles.create(roleName, '/', trustPolicy);
      roles.checkExist(roleName, true);
    });

    it('should edit rgw role', () => {
      roles.edit(roleName, 3);
    });

    it('should delete rgw role', () => {
      roles.deleteRole(roleName);
      roles.checkExist(roleName, false);
    });
  });
});
