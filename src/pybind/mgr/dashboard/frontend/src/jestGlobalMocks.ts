Object.defineProperty(window, 'getComputedStyle', {
  value: () => ({
    getPropertyValue: () => {
      return '';
    }
  })
});

/* Carbon Charts (via @carbon/charts or @carbon/charts-angular) uses ResizeObserver
  to automatically adjust chart size on container resize, and it expects it to be
  available globally — which isn’t the case in unit tests.
*/
Object.defineProperty(window, 'ResizeObserver', {
  writable: true,
  configurable: true,
  value: class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
});

Object.defineProperty(SVGElement.prototype, 'getComputedTextLength', {
  configurable: true,
  value: () => 100 // or any realistic number
});
