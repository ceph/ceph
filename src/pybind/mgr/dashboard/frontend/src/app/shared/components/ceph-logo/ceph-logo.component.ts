import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-ceph-logo',
  templateUrl: './ceph-logo.component.html',
  styleUrls: ['./ceph-logo.component.scss']
})

export class CephLogo {
  @Input() variant: string;
}
