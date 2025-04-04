set -ex
HOST_IP=$(hostname -i)
sudo tee -a /tmp/dex.conf > /dev/null << EOF
        issuer: http://${HOST_IP}:5556/dex

        storage:
          type: sqlite3
          config:
            file: /var/dex/dex.db

        web:
          http: 0.0.0.0:5556

        expiry:
          deviceRequests: 5h
          signingKeys: 6h
          idTokens: 24h
          authRequests: 24h

        logger:
          level: info
          format: text

        oauth2:
          responseTypes: [code]
          skipApprovalScreen: false
          alwaysShowLoginScreen: false

        enablePasswordDB: true

        staticClients:
          - id: oauth2-proxy
            redirectURIs:
              - 'https://${MGMT_GW_HOST}/oauth2/callback'
            name: 'oauth2-proxy'
            secret: proxy

        connectors:
          - type: mockCallback
            id: mock
            name: Example

        staticPasswords:
          - email: "admin@example.com"
            hash: "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
            username: "admin"
            userID: "08a8684b-db88-4b73-90a9-3cd1661f5466"
EOF
podman run -d --rm -p 5556:5556 -p 5555:5555 -v /tmp/dex.conf:/etc/dex/config.docker.yaml --name dex-container ghcr.io/dexidp/dex
