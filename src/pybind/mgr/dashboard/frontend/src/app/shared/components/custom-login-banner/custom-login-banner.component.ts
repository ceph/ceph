import { Component, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { CustomLoginBannerService } from '~/app/shared/api/custom-login-banner.service';

@Component({
  selector: 'cd-custom-login-banner',
  templateUrl: './custom-login-banner.component.html',
  styleUrls: ['./custom-login-banner.component.scss']
})
export class CustomLoginBannerComponent implements OnInit {
  bannerText$: Observable<string>;
  constructor(private customLoginBannerService: CustomLoginBannerService) {}

  ngOnInit(): void {
    this.bannerText$ = this.customLoginBannerService.getBannerText();
  }
}
