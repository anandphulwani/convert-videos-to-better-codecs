# 🎥 AV1 Distributed Encoding Job Processor

This project is a **highly parallelized, fault-tolerant, and distributed video encoding pipeline** designed to process `.mp4` files using the **AV1 codec** at multiple CRF (Constant Rate Factor) values. It manages file discovery, chunking, multi-pass encoding, real-time progress tracking, pause/resume logic, and error recovery.

---

## 🚀 Features

- ⚙️ **Multi-process encoding** using Python `multiprocessing`
- 📦 **Chunk-based task division** and job claiming
- 🎛️ **Per-CRF encoding** via FFmpeg for quality comparisons
- 📊 **Real-time UI** using `tqdm` with per-worker and per-chunk bars
- 🧠 **Pause/resume support** with safe process shutdowns
- 🧹 **Auto-cleanup of failed or skipped jobs**
- 📁 **Flexible file structure** for input/output control
- 🔒 **Concurrency-safe locks** for remote file claiming
- 🧾 **Logging system** for traceability and debugging

---

## 📁 Project Structure

```
.
├── main.py                          # Entry point
├── config.py                        # Configuration & CLI parsing
├── clazz/
│   └── JobManager.py                # Job management & process control
├── includes/
│   ├── encode_file.py              # Encoding logic (FFmpeg)
│   └── cleanup_working_folders.py  # Cleanup helpers
├── tqdm_manager.py                 # Progress bar manager (tqdm)
├── helpers/                        # Utility functions (logging, I/O)
└── README.md
```

---

## ⚙️ Configuration (`config.py`)

You can modify paths, chunk sizes, CRF values, and worker counts here.

### Notable Variables

|----------------------------|------------------------------------------------|
| Variable                   | Description                                    |
|----------------------------|------------------------------------------------|
| `CRF_VALUES`               | CRF levels to encode with (e.g. `[24, 60]`)    |
| `MAX_WORKERS`              | Number of parallel encoding workers            |
| `CHUNK_SIZE`               | Chunk size in bytes                            |
| `REMOTE_ROOT`              | Remote job directory                           |
| `TMP_*`                    | Temporary directories for input/output         |
| `FINAL_OUTPUT_ROOT`        | Final destination of encoded files             |
| `MACHINE_ID`               | Auto-generated or env-provided machine ID      |
|----------------------------|------------------------------------------------|

---

## 🛠️ Installation (Tested on Proxmox)

### 1. Mount the remote as a local path

```bash
apt update -y && apt install -y sshfs
mkdir /mnt/AAA.BBB.CCC.DDD_remote # Replace `AAA.BBB.CCC.DDD` with the IP of remote machine in all the commands
sshfs -o allow_other root@AAA.BBB.CCC.DDD:/ /mnt/AAA.BBB.CCC.DDD_remote/ # use this to test if connection works
ls -al /mnt/AAA.BBB.CCC.DDD_remote/
umount /mnt/AAA.BBB.CCC.DDD_remote/ # remove after successfull connections
printf '%s' 'YOUR_PASSWORD' > /root/.sshfs_AAA.BBB.CCC.DDD_remote_password # replace `YOUR_PASSWORD` with the actual password
chmod 600 /root/.sshfs_AAA.BBB.CCC.DDD_remote_password
# Replace `/path/to/remote_dir` with actual path in the below command
cat >/etc/systemd/system/AAA.BBB.CCC.DDD_remote.service <<'EOF'
[Unit]
Description=SSHFS mount to /mnt/AAA.BBB.CCC.DDD_remote
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/bin/sh -c 'cat /root/.sshfs_AAA.BBB.CCC.DDD_remote_password | /usr/bin/sshfs root@AAA.BBB.CCC.DDD:/path/to/remote_dir /mnt/AAA.BBB.CCC.DDD_remote -f -o allow_other,StrictHostKeyChecking=no,reconnect,ServerAliveInterval=15,ServerAliveCountMax=3,password_stdin'
ExecStop=/bin/fusermount -u /mnt/remote
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now AAA.BBB.CCC.DDD_remote.service
```

### 1. Clone the Repository and Run

```bash
apt install git -y
git clone https://github.com/anandphulwani/convert-videos-to-better-codecs
```

### 2. Install System Dependencies

```bash
apt install ffmpeg
apt install libx264-dev libx265-dev libopus0 libmp3lame0 -y
apt install libsvtav1enc1 -y || apt install libsvtav1enc2 -y
apt install python3-tqdm python3-psutil -y
```

---

## 🧪 Usage

To start the encoding job processor:

```bash
cd convert-videos-to-better-codecs
cp config.py.template config.py # Copy the config template file and change values
which python3 # Check the path of it and use it while calling main.py
clear && nice -n 19 /usr/bin/python3 main.py # Add `--debug` and `--throttle` as required
```

Optional flags:

```
--debug        # Enables detailed logging
--throttle     # Enables CPU watchdog for throttling
```

---

## 📊 Output Structure

```
/mnt/111.222.111.222_remote/Sample_Videos_Repo_Directory/
├── jobs/
│   ├── to_assign/         # Files to be processed
│   ├── in_progress/       # Currently processing
│   ├── done/              # Completed encodings
│   ├── failed/            # Failed encodings
│   └── logs/              # Per-machine logs
├── locks/
```

---

## 📈 Progress Example

```text
Slot 01 | [24] sample1.mp4      ████████  82MB/100MB [00:24<00:05, 3.5MB/s]
Slot 02 | Waiting...
Chunk 01                        ████████ 145MB/150MB ✓ size=145MB • elapsed=00:39
```

---

## 🧩 How It Works

1. **`main.py`** starts the job manager and attaches UI bars.
2. **`JobManager`** discovers input chunks and distributes encoding tasks.
3. **Workers** encode each file using `encode_file.py` and FFmpeg.
4. **Progress updates** are pushed via `multiprocessing.Queue` to `tqdm_manager`.
5. **Chunks are finalized** when all CRFs are successfully encoded.
6. **Failed/skipped files** are moved or logged accordingly.

---

## ⏸️ Pause/Resume Support

- A CPU watchdog can trigger an auto-pause if usage exceeds a threshold.
- While paused:
  - Workers stop gracefully
  - `TMP_PROCESSING` is cleaned
  - Logs are flushed centrally
- Upon resume:
  - Jobs restart using the `TMP_INPUT` files which are not processed.

---

## 🧹 Cleanup Behavior

- On exit (normal or interrupted), the following are cleaned:
  - Local temp directories (`TMP_PROCESSING`)
  - Incomplete logs
  - Terminal display (tqdm bars)
  - Lock files (if any)

---

## 📬 Need More?

Would you like us to include:
- Logging format documentation?
- Retry policies for failures?
- Sample `.env` file for environment overrides?
- A systemd or Docker setup for deployment?

Let us know!

