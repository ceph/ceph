import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class LinkProviderService {

  constructor() { }

  telemetrycomponent1 () {
     const url = `https://telemetry-public.ceph.com/`
    return url;
  };

  telemetrycomponent2 () {
   const url =  `https://cdla.io/sharing-1.0/`
  return url;
  };

  docservice1 () {
    const url =  " https://docs.ceph.com/en/${docVersion}/ "
    return url;
  };

  docservice2 () {
    const url = `https://ceph.io/`
    return url;
  };

  telemetrynotificationcomponent () {
    const url =  `https://docs.ceph.com/en/latest/mgr/telemetry/`
    return url;
  };

  iscsitargetformcomponent() {
    const url = `https://en.wikipedia.org/wiki/ISCSI#Addressing`
    return url;
  };
}
