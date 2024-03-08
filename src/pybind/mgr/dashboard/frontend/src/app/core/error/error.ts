import { Icons } from '~/app/shared/enum/icons.enum';

export class DashboardError extends Error {
  header: string;
  message: string;
  icon: string;
}

export class DashboardNotFoundError extends DashboardError {
  header = $localize`Page Not Found`;
  message = $localize`Sorry, we couldn’t find what you were looking for.
  The page you requested may have been changed or moved.`;
  icon = Icons.warning;
}

export class DashboardForbiddenError extends DashboardError {
  header = $localize`Access Denied`;
  message = $localize`Sorry, you don’t have permission to view this page or resource.`;
  icon = Icons.lock;
}

export class DashboardUserDeniedError extends DashboardError {
  header = $localize`User Denied`;
  message = $localize`Sorry, the user does not exist in Ceph.
  You'll be logged out from the Identity Provider when you retry logging in.`;
  icon = Icons.warning;
}
