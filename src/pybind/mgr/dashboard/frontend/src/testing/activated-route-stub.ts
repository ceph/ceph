/* eslint-disable @typescript-eslint/member-ordering */
import { ActivatedRoute } from '@angular/router';

import { ReplaySubject } from 'rxjs';

/**
 * An ActivateRoute test double with a `params` observable.
 * Use the `setParams()` method to add the next `params` value.
 */
export class ActivatedRouteStub extends ActivatedRoute {
  // Use a ReplaySubject to share previous values with subscribers
  // and pump new values into the `params` observable
  private subject = new ReplaySubject<Record<string, unknown>>();

  constructor(initialParams?: Record<string, unknown>) {
    super();
    this.setParams(initialParams);
  }

  /** The mock params observable */
  readonly params = this.subject.asObservable();

  /** Set the params observables's next value */
  setParams(params?: Record<string, unknown>) {
    this.subject.next(params);
  }
}
