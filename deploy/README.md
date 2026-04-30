# 3FS Setup Guide

This section provides a manual deployment guide for setting up a six-node cluster with the cluster ID `stage`.

## Installation prerequisites

### Hardware specifications

| Machine | Node    | OS           | IP            | Memory | SSD     | RDMA |
| ------- | ------- | ------------ | ------------- | ------ | ------- | ---- |
| xsel01  | meta    | Ubuntu 22.04 | 192.168.100.1 | 128GB  | 2TB × 2 | RoCE |
| xsel01  | storage | Ubuntu 22.04 | 192.168.100.1 | 128GB  | 2TB × 2 | RoCE |
| xsel02  | client | Ubuntu 22.04 | 192.168.100.2 | 128GB  | 2TB × 1 | RoCE |

> **RDMA Configuration**
> 1. Assign IP addresses to RDMA NICs. Multiple RDMA NICs (InfiniBand or RoCE) are supported on each node.
> 2. Check RDMA connectivity between nodes using `ib_write_bw`.

### Third-party dependencies

In production environment, it is recommended to install FoundationDB and ClickHouse on dedicated nodes.

| Service    | Node |
|------------|-------------------------|
| [ClickHouse](https://clickhouse.com/docs/install) | meta |
| [FoundationDB](https://apple.github.io/foundationdb/administration.html) | meta |

> **FoundationDB**
> 1. Ensure that the version of FoundationDB client matches the server version, or copy the corresponding version of `libfdb_c.so` to maintain compatibility.
> 2. Find the `fdb.cluster` file and `libfdb_c.so` at `/etc/foundationdb/fdb.cluster`, `/usr/lib/libfdb_c.so` on nodes with FoundationDB installed.


---
## Step 0: Build 3FS

Follow the [instructions](../README.md#build-3fs) to build 3FS. Binaries can be found in `build/bin`.

### Services and clients

The following steps show how to install 3FS services in `/opt/3fs/bin` and the config files in `/opt/3fs/etc`.

| Service    | Binary                  | Config files                                                                | NodeID | Node |
|------------|-------------------------|-----------------------------------------------------------------------------|--------|---------------|
| monitor    | monitor_collector_main  | [monitor_collector_main.toml](../configs/monitor_collector_main.toml)       | -      |  meta        |
| admin_cli  | admin_cli               | [admin_cli.toml](../configs/admin_cli.toml)<br>fdb.cluster                  | -      |  meta<br>storage1<br>storage2<br>storage3<br>storage4<br>storage5 |
| mgmtd      | mgmtd_main              | [mgmtd_main_launcher.toml](../configs/mgmtd_main_launcher.toml)<br>[mgmtd_main.toml](../configs/mgmtd_main.toml)<br>[mgmtd_main_app.toml](../configs/mgmtd_main_app.toml)<br>fdb.cluster | 1            |  meta        |
| meta       | meta_main               | [meta_main_launcher.toml](../configs/meta_main_launcher.toml)<br>[meta_main.toml](../configs/meta_main.toml)<br>[meta_main_app.toml](../configs/meta_main_app.toml)<br>fdb.cluster       | 100          |  meta        |
| storage    | storage_main            | [storage_main_launcher.toml](../configs/storage_main_launcher.toml)<br>[storage_main.toml](../configs/storage_main.toml)<br>[storage_main_app.toml](../configs/storage_main_app.toml)       | 10001~10005  |  storage1<br>storage2<br>storage3<br>storage4<br>storage5 |
| client     | hf3fs_fuse_main         | [hf3fs_fuse_main_launcher.toml](../configs/hf3fs_fuse_main_launcher.toml)<br>[hf3fs_fuse_main.toml](../configs/hf3fs_fuse_main.toml)                                                     | -            |  meta        |

---
## Step 1: Create ClickHouse tables for metrics

   Import the SQL file into ClickHouse:
   ```bash
   clickhouse-client -n < ~/3fs/deploy/sql/3fs-monitor.sql
   ```

---
## Step 2: Monitor service

Install `monitor_collector` service on the **meta** node.

1. Copy `monitor_collector_main` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`, and create log directory `/var/log/3fs`.
   ```bash
   mkdir -p /opt/3fs/{bin,etc}
   mkdir -p /var/log/3fs
   cp ~/3fs/build/bin/monitor_collector_main /opt/3fs/bin
   cp ~/3fs/configs/monitor_collector_main.toml /opt/3fs/etc
   ```
2. Update [`monitor_collector_main.toml`](../configs/monitor_collector_main.toml) to add a ClickHouse connection:
   ```toml
   [server.monitor_collector.reporter]
   type = 'clickhouse'

   [server.monitor_collector.reporter.clickhouse]
   db = '3fs'
   host = '<CH_HOST>'
   passwd = '<CH_PASSWD>'
   port = '<CH_PORT>'
   user = '<CH_USER>'
   ```
3. Start monitor service:
   ```bash
   cp ~/3fs/deploy/systemd/monitor_collector_main.service /usr/lib/systemd/system
   systemctl start monitor_collector_main
   ```

Note that
> - Multiple instances of monitor services can be deployed behind a virtual IP address to share the traffic.
> - Other services communicate with the monitor service over a TCP connection.

---
## Step 3: Admin client
Install `admin_cli` on **all** nodes.

1. Copy `admin_cli` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`.
   ```bash
   mkdir -p /opt/3fs/{bin,etc}
   rsync -avz meta:~/3fs/build/bin/admin_cli /opt/3fs/bin
   rsync -avz meta:~/3fs/configs/admin_cli.toml /opt/3fs/etc
   rsync -avz meta:/etc/foundationdb/fdb.cluster /opt/3fs/etc
   ```
2. Update [`admin_cli.toml`](../configs/admin_cli.toml) to set `cluster_id` and `clusterFile`:
   ```toml
   cluster_id = "stage"

   [fdb]
   clusterFile = '/opt/3fs/etc/fdb.cluster'
   ```

The full help documentation for `admin_cli` can be displayed by running the following command:

```bash
/opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml help
```

---
## Step 4: Mgmtd service
Install `mgmtd` service on **meta** node.

1. Copy `mgmtd_main` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`.
   ```bash
   cp ~/3fs/build/bin/mgmtd_main /opt/3fs/bin
   cp ~/3fs/configs/{mgmtd_main.toml,mgmtd_main_launcher.toml,mgmtd_main_app.toml} /opt/3fs/etc
   ```
2. Update config files:
   - Set mgmtd `node_id = 1` in [`mgmtd_main_app.toml`](../configs/mgmtd_main_app.toml).
   - Edit [`mgmtd_main_launcher.toml`](../configs/mgmtd_main_launcher.toml) to set the `cluster_id` and `clusterFile`:
   ```toml
   cluster_id = "stage"

   [fdb]
   clusterFile = '/opt/3fs/etc/fdb.cluster'
   ```
   - Set monitor address in [`mgmtd_main.toml`](../configs/mgmtd_main.toml):
   ```toml
   [common.monitor.reporters.monitor_collector]
   remote_ip = "192.168.100.1:10000"
   ```
3. Initialize the cluster:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml "init-cluster --mgmtd /opt/3fs/etc/mgmtd_main.toml 1 4194304 2"
   ```

   The parameters of `admin_cli`:
   > - `1` the chain table ID
   > - `1048576` the chunk size in bytes
   > - `16` the file strip size

   Run `help init-cluster` for full documentation.
4. Start mgmtd service:
   ```bash
   cp ~/3fs/deploy/systemd/mgmtd_main.service /usr/lib/systemd/system
   systemctl start mgmtd_main
   ```
5. Run `list-nodes` command to check if the cluster has been successfully initialized:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "list-nodes"
   ```

If multiple instances of `mgmtd` services deployed, one of the `mgmtd` services is elected as the primary; others are secondaries. Automatic failover occurs when the primary fails.

---
## Step 5: Meta service
Install `meta` service on **meta** node.
1. Copy `meta_main` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`.
   ```bash
   cp ~/3fs/build/bin/meta_main /opt/3fs/bin
   cp ~/3fs/configs/{meta_main_launcher.toml,meta_main.toml,meta_main_app.toml} /opt/3fs/etc
   ```
2. Update config files:
   - Set meta `node_id = 100` in [`meta_main_app.toml`](../configs/meta_main_app.toml).
   - Set `cluster_id`, `clusterFile` and mgmtd address in [`meta_main_launcher.toml`](../configs/meta_main_launcher.toml):
   ```toml
   cluster_id = "stage"

   [mgmtd_client]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]
   ```
   - Set mgmtd and monitor addresses in [`meta_main.toml`](../configs/meta_main.toml).
   ```toml
   [server.mgmtd_client]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]

   [common.monitor.reporters.monitor_collector]
   remote_ip = "192.168.100.1:10000"

   [server.fdb]
   clusterFile = '/opt/3fs/etc/fdb.cluster'
   ```
3. Config file of meta service is managed by mgmtd service. Use `admin_cli` to upload the config file to mgmtd:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "set-config --type META --file /opt/3fs/etc/meta_main.toml"
   ```
4. Start meta service:
   ```bash
   cp ~/3fs/deploy/systemd/meta_main.service /usr/lib/systemd/system
   systemctl start meta_main
   ```
5. Run `list-nodes` command to check if meta service has joined the cluster:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "list-nodes"
   ```

If multiple instances of `meta` services deployed, meta requests will be evenly distributed to all instances.

---
## Step 6: Storage service
Install `storage` service on **storage** node.
1. Format the attached SSDs as XFS and mount them at `/storage/data{1..2}`, then create data directories `/storage/data{1..2}/3fs` and log directory `/var/log/3fs`.
   For a single-node setup, map `/dev/nvme0n1` to `/storage/data1` and `/dev/nvme1n1` to `/storage/data2`:
   ```bash
   mkdir -p /var/log/3fs
   mkdir -p /storage/data1 /storage/data2

   umount /storage/data1 || true
   umount /storage/data2 || true

   mkfs.xfs -f -L data1 -s size=4096 /dev/nvme0n1
   mkfs.xfs -f -L data2 -s size=4096 /dev/nvme1n1

   mount -o noatime,nodiratime -L data1 /storage/data1
   mount -o noatime,nodiratime -L data2 /storage/data2

   mkdir -p /storage/data1/3fs /storage/data2/3fs
   ```
2. Increase the max number of asynchronous aio requests:
   ```bash
   sysctl -w fs.aio-max-nr=67108864
   ```
3. Copy `storage_main` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`.
   ```bash
   rsync -avz meta:~/3fs/build/bin/storage_main /opt/3fs/bin
   rsync -avz meta:~/3fs/configs/{storage_main_launcher.toml,storage_main.toml,storage_main_app.toml} /opt/3fs/etc
   ```
4. Update config files:
   - Set `node_id` in [`storage_main_app.toml`](../configs/storage_main_app.toml). Each storage service is assigned a unique id between `10001` and `10005`.
   - Set `cluster_id` and mgmtd address in [`storage_main_launcher.toml`](../configs/storage_main_launcher.toml).
   ```toml
   cluster_id = "stage"

   [mgmtd_client]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]
   ```
   - Add target paths in [`storage_main.toml`](../configs/storage_main.toml):
   ```toml
   [server.mgmtd]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]

   [common.monitor.reporters.monitor_collector]
   remote_ip = "192.168.100.1:10000"

   [server.targets]
   target_paths = ["/storage/data1/3fs","/storage/data2/3fs",]
   ```
5. Config file of storage service is managed by mgmtd service. Use `admin_cli` to upload the config file to mgmtd:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "set-config --type STORAGE --file /opt/3fs/etc/storage_main.toml"
   ```
6. Start storage service:
   ```bash
   rsync -avz meta:~/3fs/deploy/systemd/storage_main.service /usr/lib/systemd/system
   systemctl start storage_main
   ```
7. Run `list-nodes` command to check if storage service has joined the cluster:
   ```
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "list-nodes"
   ```

---
## Step 7: Create admin user, storage targets and chain table
1. Create an admin user:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "user-add --root --admin 0 root"
   ```
   The admin token is printed to the console, save it to `/opt/3fs/etc/token.txt`.
2. Generate `admin_cli` commands to create storage targets and a simple chain table.
   For a single-node setup with storage node `10001` and two disks, `data_placement.py` is not required. Create one target per disk and one chain per target:
   ```bash
   mkdir -p output

   printf '%s\n' \
     'create-target --node-id 10001 --disk-index 0 --target-id 1000101001 --chain-id 1000101001' \
     'create-target --node-id 10001 --disk-index 1 --target-id 1000102001 --chain-id 1000102001' \
     > output/create_target_cmd.txt

   printf '%s\n' \
     'ChainId,TargetId' \
     '1000101001,1000101001' \
     '1000102001,1000102001' \
     > output/generated_chains.csv

   printf '%s\n' \
     'ChainId' \
     '1000101001' \
     '1000102001' \
     > output/generated_chain_table.csv
   ```
   The following 3 files will be generated in `output` directory: `create_target_cmd.txt`, `generated_chains.csv`, and `generated_chain_table.csv`.
3. Create storage targets:
   ```bash
   /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' --config.user_info.token $(<"/opt/3fs/etc/token.txt") < output/create_target_cmd.txt
   ```
4. Upload chains to mgmtd service:
   ```bash
   /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' --config.user_info.token $(<"/opt/3fs/etc/token.txt") "upload-chains output/generated_chains.csv"
   ```
5. Upload chain table to mgmtd service:
    ```bash
    /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' --config.user_info.token $(<"/opt/3fs/etc/token.txt") "upload-chain-table 1 output/generated_chain_table.csv --desc replica-1"
    ```
6. List chains and chain tables to check if they have been correctly uploaded:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "list-chains"
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "list-chain-tables"
   ```
---
## Step 8: FUSE client
For simplicity FUSE client is deployed on the **meta** node in this guide. However, we strongly advise against deploying clients on service nodes in production environment.

1. Copy `hf3fs_fuse_main` to `/opt/3fs/bin` and config files to `/opt/3fs/etc`.
   ```bash
   cp ~/3fs/build/bin/hf3fs_fuse_main /opt/3fs/bin
   cp ~/3fs/configs/{hf3fs_fuse_main_launcher.toml,hf3fs_fuse_main.toml,hf3fs_fuse_main_app.toml} /opt/3fs/etc
   ```
2. Create the mount point:
   ```bash
   mkdir -p /3fs/stage
   ```
3. Set cluster ID, mountpoint, token file and mgmtd address in [`hf3fs_fuse_main_launcher.toml`](../configs/hf3fs_fuse_main_launcher.toml)
   ```toml
   cluster_id = "stage"
   mountpoint = '/3fs/stage'
   token_file = '/opt/3fs/etc/token.txt'

   [mgmtd_client]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]
   ```
4. Set mgmtd and monitor address in [`hf3fs_fuse_main.toml`](../configs/hf3fs_fuse_main.toml).
   ```toml
   [mgmtd]
   mgmtd_server_addresses = ["RDMA://192.168.100.1:8000"]

   [common.monitor.reporters.monitor_collector]
   remote_ip = "192.168.100.1:10000"
   ```
5. Config file of FUSE client is also managed by mgmtd service. Use `admin_cli` to upload the config file to mgmtd:
   ```bash
   /opt/3fs/bin/admin_cli -cfg /opt/3fs/etc/admin_cli.toml --config.mgmtd_client.mgmtd_server_addresses '["RDMA://192.168.100.1:8000"]' "set-config --type FUSE --file /opt/3fs/etc/hf3fs_fuse_main.toml"
   ```
6. Start FUSE client:
   ```bash
   cp ~/3fs/deploy/systemd/hf3fs_fuse_main.service /usr/lib/systemd/system
   systemctl start hf3fs_fuse_main
   ```
7. Check if 3FS has been mounted at `/3fs/stage`:
   ```bash
   mount | grep '/3fs/stage'
   ```

## FAQ
<details>
  <summary>How to troubleshoot <code>admin_cli init-cluster</code> error?</summary>

  If mgmtd fails to start after running `init-cluster`, the most likely cause is an error in `mgmtd_main.toml`. Any changes to this file require clearing all FoundationDB data and re-running `init-cluster`
</details>

---
<details>
  <summary>How to build a single-node cluster?</summary>

  A minimum of two storage services is required for data replication. If set `--num-nodes=1`, the `gen_chain_table.py` script will fail. In a test environment, this limitation can be bypassed by deploying multiple storage services on a single machine.
</details>

---
<details>
  <summary>How to update config files?</summary>

  All config files are managed by mgmtd. If any `*_main.toml` is updated, such as `storage_main.toml`, the modified file should be uploaded using `admin_cli set-config`.
</details>

---
<details>
  <summary>How to troubleshoot common deployment issues?</summary>

  When encountering any error during deployment,
  - Check the log messages in `stdout/stderr` using `journalctl`, especially during service startup.
  - Check log files stored in `/var/log/3fs/` on service and client nodes.
  - Ensure that the directory `/var/log/3fs/` exists before starting any service.
</details>
