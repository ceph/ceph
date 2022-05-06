import { TestBed } from '@angular/core/testing';

import { LinkProviderService } from './link-provider.service';

describe('LinkProviderService', () => {
  let service: LinkProviderService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(LinkProviderService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it ('should test for correct links', () => {
    expect(service.telemetrycomponent1()) .toBe (`https://telemetry-public.ceph.com/`);
  });
  it ('should test for correct links', () => {
    expect(service.telemetrycomponent2()) .toBe (`https://cdla.io/sharing-1.0/`);
  });
  it ('should test for correct links', () => {
    expect(service.docservice1()) .toBe ('https://docs.ceph.com/en/${docVersion}/');
  });
  it ('should test for correct links', () => {
    expect(service.docservice2()) .toBe (`https://ceph.io/`);
  });
  it ('should test for correct links', () => {
    expect(service.telemetrynotificationcomponent()) .toBe (`https://docs.ceph.com/en/latest/mgr/telemetry/`);
  });
  it ('should test for correct links', () => {
    expect(service.iscsitargetformcomponent()) .toBe (`https://en.wikipedia.org/wiki/ISCSI#Addressing`);
  });  
});
