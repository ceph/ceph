import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class CustomLoginBannerService {
  baseUiURL = 'ui-api/login/custom_banner';

  constructor(private http: HttpClient) {}

  getBannerText() {
    return this.http.get<string>(this.baseUiURL);
  }
}
