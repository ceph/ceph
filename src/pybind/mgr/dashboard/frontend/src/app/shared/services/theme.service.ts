import { DOCUMENT } from '@angular/common';
import { Injectable, Inject, Renderer2, RendererFactory2 } from '@angular/core';

interface Theme {
  label: string;
  icon: string;
}

@Injectable({
  providedIn: 'root'
})
export class ThemeService {
  private renderer: Renderer2;
  style: HTMLLinkElement;
  currentTheme: Theme;

  themes = [
    {
      label: 'light',
      icon: 'fa-sun-o'
    },
    {
      label: 'dark',
      icon: 'fa-moon-o'
    }
  ];

  constructor(
    @Inject(DOCUMENT) private document: Document,
    private rendererFactory: RendererFactory2
  ) {
    this.renderer = this.rendererFactory.createRenderer(null, null);
    this.currentTheme = this.themes[0];
    this.style = this.renderer.createElement('link');
    this.style.rel = 'stylesheet';
    this.renderer.appendChild(this.document.head, this.style);
    // this.changeTheme(this.currentTheme);
  }

  getTheme(): Theme {
    return this.currentTheme;
  }

  getAllThemes(): Theme[] {
    return this.themes;
  }

  setTheme(theme: Theme) {
    this.currentTheme = theme;
  }

  changeTheme(theme: Theme): void {
    this.style.href = `${theme.label}.css`;
    this.setTheme(theme);
  }
}
