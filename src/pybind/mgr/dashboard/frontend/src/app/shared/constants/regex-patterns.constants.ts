export const IPV4VALIDATIONREGEX =
  /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;

// IPv6 validation regex - validates a complete IPv6 address string
export const IPV6VALIDATIONREGEX = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;

// IPv4 extraction regex - extracts IPv4 address from a string (e.g., from URLs)
export const IPV4EXTRACTIONREGEX = /^(\d{1,3}(?:\.\d{1,3}){3})/;

// IPv6 extraction regex - extracts IPv6 address from bracketed notation (e.g., [::1])
export const IPV6EXTRACTIONREGEX = /^\[([a-fA-F0-9:]+)\]/;
