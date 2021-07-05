import { Component, OnInit } from '@angular/core';

import { Motd, MotdService } from '~/app/shared/api/motd.service';

@Component({
  selector: 'cd-motd',
  templateUrl: './motd.component.html',
  styleUrls: ['./motd.component.scss']
})
export class MotdComponent implements OnInit {
  motd?: Motd;

  constructor(private motdService: MotdService) {}

  ngOnInit(): void {
    this.motdService.get().subscribe((res?: Motd) => {
      if (res) {
        this.motd = res;
      }
    });
  }
}
