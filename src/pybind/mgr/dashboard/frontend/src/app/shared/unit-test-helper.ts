import { async, TestBed } from '@angular/core/testing';

import { _DEV_ } from '../../unit-test-configuration';

export function configureTestBed(configuration, useOldMethod?) {
  if (_DEV_ && !useOldMethod) {
    const resetTestingModule = TestBed.resetTestingModule;
    beforeAll((done) =>
      (async () => {
        TestBed.resetTestingModule();
        TestBed.configureTestingModule(configuration);
        // prevent Angular from resetting testing module
        TestBed.resetTestingModule = () => TestBed;
      })()
        .then(done)
        .catch(done.fail));
    afterAll(() => {
      TestBed.resetTestingModule = resetTestingModule;
    });
  } else {
    beforeEach(async(() => {
      TestBed.configureTestingModule(configuration);
    }));
  }
}
