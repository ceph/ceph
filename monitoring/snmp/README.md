# SNMP schema

## Traps

| OID | Description |
| :--- | :--- |
| 1.3.6.1.4.1.50495.15.1.2.1 | The default trap. This is used if no OID is specified in the alert labels. |
| 1.3.6.1.4.1.50495.15.1.2.[2...N] | Custom traps. |

## Objects

The following objects are appended as variable binds to an SNMP trap.

| OID | Type | Description |
| :--- | :---: | :--- |
| 1.3.6.1.4.1.50495.15.1.1.1 | String | The name of the Prometheus alert. |
| 1.3.6.1.4.1.50495.15.1.1.2 | String | The status of the Prometheus alert. |
| 1.3.6.1.4.1.50495.15.1.1.3 | String | The severity of the Prometheus alert. |
| 1.3.6.1.4.1.50495.15.1.1.4 | String | Unique identifier for the Prometheus instance. |
| 1.3.6.1.4.1.50495.15.1.1.5 | String | The name of the Prometheus job. |
| 1.3.6.1.4.1.50495.15.1.1.6 | String | The Prometheus alert description field. |
| 1.3.6.1.4.1.50495.15.1.1.7 | String | Additional Prometheus alert labels as JSON string. |
| 1.3.6.1.4.1.50495.15.1.1.8 | Unix timestamp | The time when the Prometheus alert occurred. |
| 1.3.6.1.4.1.50495.15.1.1.9 | String | The raw Prometheus alert as JSON string. |