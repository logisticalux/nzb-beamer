# NZB Beamer

A self-hosted Node.js/Express web app for beaming `.nzb` files straight to a Windows machine running [NZBGet](https://nzbget.com/) — and for managing the media library that grows around it. Built as a small dark-themed single-page app with no external dependencies beyond a browser, plus optional deep integration with [Home Assistant](https://www.home-assistant.io/) via MQTT.

<img width="1494" height="3426" alt="nzb-beamer" src="https://github.com/user-attachments/assets/d5e2aab2-e313-4fa3-a252-9ec60d389961" />

## What it does

Drop an `.nzb` file into the web UI (from any device on the network) and NZB Beamer:

1. Parses it and shows size, file count and usenet groups
2. Saves it into a watched folder so NZBGet can pick it up
3. Tracks the real download progress by polling NZBGet's JSON-RPC API
4. Automatically moves the finished download into the right media folder
5. Deletes the processed `.nzb` file once the download is done
6. Reports all of the above — plus full server/hardware telemetry — to Home Assistant

## Features

### 📤 Direct upload ("Beaming")
- Drag-and-drop / multi-file selection from the browser, uploaded to the server in one batch
- Live NZB metadata: filename, total size, file count, usenet groups
- Real download progress pulled directly from NZBGet (percent, MB downloaded/total, status) — not a bandwidth estimate
- When several files are beamed at once, the UI always surfaces whichever one is *currently* downloading
- Configurable "beam drop folder" (where uploaded `.nzb` files are saved), changeable from the UI instead of being hardcoded
- Finished `.nzb` files are automatically deleted from the drop folder once NZBGet reports them done
- Full NZBGet queue view (name + size for every queued/active item, not just the one you just sent)

### 📁 Category-based library management
- Built-in categories (Movies / TV / Music), each with its own **source** and **target** directory
- Add unlimited custom categories on the fly — adding one automatically creates a matching source *and* target slot
- Built-in Windows folder browser (drive selector, breadcrumb navigation, no native OS dialogs required) for picking directories remotely
- Live file/folder listing for every configured source and target directory
- Chosen directories persist across restarts

### 🚚 Automated & manual file moving
- Background job periodically moves whole folders (not just loose files) from each source directory into its matching target directory
- No recursive file-hunting and no stray subfolders created — folders are moved as complete units
- Manual "move now" trigger available from the UI at any time
- Move results (per category: moved / partial / failed / nothing to do) shown live

### 🧹 Empty-folder cleanup
- On-demand scan across *every* configured target directory for leftover empty subfolders
- Results shown as a checklist — select the ones you want gone and delete them in one click
- Safety checks: only deletes directories that are still empty and actually live inside a configured target directory

### 📊 Server dashboard & logging
- Self-drawing console dashboard (no endless scrolling log) showing live status at a glance
- Persistent action log of every beamed NZB (timestamp, source IP, filename, size, file count, groups), viewable from the UI
- Restart / shutdown the server itself from the web UI

### 🔒 Access control
- LAN-only access restriction via IP allow-list (supports both prefix ranges and exact IPs)

### 🏠 Home Assistant integration (MQTT Discovery)
Auto-discovered on connect, with Last-Will-and-Testament availability and retained state — no YAML configuration needed on the Home Assistant side.

- Online/availability binary sensor
- File-count sensor for every source/target directory, with path & size attributes
- Current NZB sensor (name, size, groups, received time)
- NZBGet download-progress sensor
- NZBGet reachability binary sensor
- NZBGet queue-length sensor (with full item list as attributes)
- **"All downloads finished" binary sensor** — flips on the moment the NZBGet queue empties out
- Last-move-result sensor (per-category outcome of the most recent move run)
- Last-access / client-IP sensors
- Total-downloads counter (all-time, survives restarts)
- Full hardware telemetry: CPU load/model/cores, RAM usage, per-disk usage, disk read/write throughput, network up/down (plus cumulative totals), CPU temperature, OS uptime and app uptime

### 🎨 Interface
- Dark, responsive UI that scales from phone to tablet to desktop (not just a shrunk phone layout)
- Animated SVG "beam" logo that reacts visually while a download is active
- Collapsible sections (directories, hardware panel, log, empty-folder scan) — collapsed by default to keep things tidy
- Toast notifications and confirmation prompts for anything destructive

## Tech stack

- **Backend**: Node.js, Express, `express-fileupload`, `fast-xml-parser`, `mqtt`, `systeminformation`
- **Frontend**: Single-file HTML/CSS/JS, no build step, no frameworks
- **Integrations**: NZBGet JSON-RPC API, MQTT / Home Assistant Discovery

## Requirements

- Node.js
- A running [NZBGet](https://nzbget.com/) instance on the same machine (or reachable over the network)
- (Optional) An MQTT broker, e.g. Home Assistant's built-in Mosquitto add-on, for the Home Assistant integration
