# EDC Instance Templates

## Direct MySQL

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "cloud",
  "password": "your_password",
  "db": "cloud",
  "charset": "utf8mb4",
  "table": "edc_data",
  "time_column": "create_time",
  "name_column": "edc_name",
  "value_column": "service_size",
  "exclude_like": "%-backup",
  "wildcard_mode": "prefix",
  "daily_rank_index": 14
}
```

## MySQL over SSH (key file)

```json
{
  "host": "127.0.0.1",
  "port": 3307,
  "user": "cloud",
  "password": "your_password",
  "db": "cloud",
  "charset": "utf8mb4",
  "table": "traffic_5m",
  "time_column": "create_time",
  "name_column": "edc_name",
  "value_column": "service_size",
  "exclude_like": "%-backup",
  "wildcard_mode": "prefix",
  "daily_rank_index": 14,
  "ssh_enabled": true,
  "ssh_host": "jump.example.com",
  "ssh_port": 22,
  "ssh_user": "root",
  "ssh_pkey": "C:/Users/you/.ssh/id_rsa",
  "ssh_allow_agent": false,
  "ssh_legacy_rsa": true,
  "ssh_remote_host": "127.0.0.1",
  "ssh_remote_port": 3307,
  "ssh_connect_retries": 3,
  "ssh_retry_delay_ms": 800
}
```

## Common Mistakes

- `ssh_pkey` uses `ssh-rsa AAAA...` public key text instead of private key file path.
- `ssh_remote_port` uses local port but remote DB listens on another port.
- Missing `exclude_like` causes backup nodes to be included unexpectedly.
