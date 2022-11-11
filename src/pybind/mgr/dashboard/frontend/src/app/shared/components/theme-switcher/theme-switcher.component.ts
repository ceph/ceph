import { Component, OnInit } from '@angular/core';
import { ThemeService } from '~/app/shared/services/theme.service';

interface Theme {
  label: string;
  icon: string;
}

@Component({
  selector: 'cd-theme-switcher',
  templateUrl: './theme-switcher.component.html',
  styleUrls: ['./theme-switcher.component.scss']
})
export class ThemeSwitcherComponent implements OnInit {
  theme: Theme;
  themes: Theme[];

  constructor(private themeService: ThemeService) {}

  ngOnInit(): void {
    this.getTheme();
    this.getAllThemes();
  }

  getTheme() {
    this.theme = this.themeService.getTheme();
  }

  getAllThemes() {
    this.themes = this.themeService.getAllThemes();
  }

  changeTheme(theme: Theme) {
    this.themeService.changeTheme(theme);
    this.getTheme();
  }
}
