import { ActivatedRoute } from '@angular/router';

import { ReplaySubject } from 'rxjs';

/**
 * An ActivateRoute test double with a `params` and `queryParams` observable.
 * Use the `setParams()` method to add the next `params` value.
 * Use the `setQueryParams()` method to add the next `params` value.
 */
export class ActivatedRouteStub extends ActivatedRoute {
  // Use a ReplaySubject to share previous values with subscribers
  // and pump new values into the `params` observable
  private paramSubject = new ReplaySubject<object>();
  private queryParamSubject = new ReplaySubject<object>();

  constructor(initialParams?: object, initialQueryParams?: object) {
    super();
    this.setParams(initialParams);
    this.setQueryParams(initialQueryParams);
  }

  /** The mock params observable */
  readonly params = this.paramSubject.asObservable();
  /** The mock queryParams observable */
  readonly queryParams = this.queryParamSubject.asObservable();

  /** Set the params observables's next value */
  setParams(params?: object) {
    this.paramSubject.next(params);
  }
  /** Set the queryParams observables's next value */
  setQueryParams(queryParams?: object) {
    this.queryParamSubject.next(queryParams);
  }
}
