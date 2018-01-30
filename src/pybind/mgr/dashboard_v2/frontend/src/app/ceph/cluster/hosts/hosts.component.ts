import { Component, OnInit } from '@angular/core';
import { HostService } from '../../../shared/services/host.service';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss']
})
export class HostsComponent implements OnInit {

  hosts: any = [];

  constructor(private hostService: HostService) { }

  ngOnInit() {
    this.hostService.list().then((resp) => {
      this.hosts = resp;
    });
  }

}
