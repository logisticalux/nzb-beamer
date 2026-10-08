const express = require('express');
const fileUpload = require('express-fileupload');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const os = require('os');
const http = require('http');
const https = require('https');
const { spawn } = require('child_process');
const mqtt = require('mqtt');
const si = require('systeminformation');
const { XMLParser } = require('fast-xml-parser');

const app = express();
const PORT = 3000;

const UPLOAD_DIR = 'C:\\Users\\loglux\\Downloads';
const CONFIG_PATH = path.join(__dirname, 'config.json');
const ACTION_LOG_PATH = path.join(__dirname, 'aktionen.log');

// Fest eingebaute Kategorien. Darüber hinaus können beliebig viele weitere
// Kategorien (Quelle + zugehöriges Ziel) über /api/categories angelegt
// werden – diese landen in config.categories und werden mit den Basis-
// Kategorien zusammengeführt (siehe getCategories()).
const BASE_CATEGORIES = [
    { key: 'filme', label: 'Filme', mdiIcon: 'mdi:movie', emoji: '🎬' },
    { key: 'serien', label: 'Serien', mdiIcon: 'mdi:television-classic', emoji: '📺' },
    { key: 'musik', label: 'Musik', mdiIcon: 'mdi:music', emoji: '🎵' },
];

function getCategories() {
    return BASE_CATEGORIES.concat((config && config.categories) || []);
}
function getCategoryKeys() {
    return getCategories().map((c) => c.key);
}
function categoryLabel(key) {
    const c = getCategories().find((x) => x.key === key);
    return c ? c.label : key;
}
function categoryIcon(key) {
    const c = getCategories().find((x) => x.key === key);
    return c ? c.mdiIcon : 'mdi:folder';
}

function slugifyCategoryKey(label) {
    const base = String(label)
        .toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // Umlaute etc. entschärfen
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '') || 'kategorie';
    let key = base;
    let i = 2;
    while (getCategoryKeys().includes(key)) {
        key = `${base}_${i}`;
        i++;
    }
    return key;
}

// Standard-Werte für erlaubte IP-Bereiche – werden beim ersten Start in
// config.networkRanges übernommen und können danach über die UI (Network
// Settings) verwaltet werden. Ein Eintrag, der mit einem Punkt endet, wird
// als Präfix behandelt (z.B. "192.168.178." erlaubt alle 192.168.178.x),
// alles andere als exakte IP.
const DEFAULT_NETWORK_RANGES = ['192.168.178.', '100.77.148.', '100.120.183.112'];

function isIpInRanges(ip, ranges) {
    if (ip === '127.0.0.1' || ip === '::1') return true;
    return (ranges || []).some((range) => {
        if (typeof range !== 'string' || !range) return false;
        return range.endsWith('.') ? ip.startsWith(range) : ip === range;
    });
}

// -------------------------------------------------------------------------
// MQTT (Home Assistant) – Zugangsdaten
// -------------------------------------------------------------------------

const MQTT_HOST = 'homeassistant.local';
const MQTT_PORT = 1883;
const MQTT_USERNAME = 'nzbbeamer';
const MQTT_PASSWORD = 'fiesta';
const MQTT_BASE = 'nzbbeamer';
const AVAILABILITY_TOPIC = `${MQTT_BASE}/status`;
const DEVICE_ID = 'nzb_beamer';

const DEVICE_INFO = {
    identifiers: [DEVICE_ID],
    name: 'NZB Beamer',
    manufacturer: 'Logistica Lux',
    model: 'NZB Beamer Server',
};

// -------------------------------------------------------------------------
// NZBGet – JSON-RPC-Zugangsdaten (für Downloadfortschritt)
// -------------------------------------------------------------------------

const NZBGET_HOST = '127.0.0.1';
const NZBGET_PORT = 6789;
const NZBGET_USERNAME = 'nzbget';
const NZBGET_PASSWORD = 'tegbzn6789';
const NZBGET_POLL_INTERVAL_MS = 8000;

let mqttClient = null;
let lastClientIp = null;
// Protokoll der letzten echten Zugriffe (Zeit + IP), neueste zuerst. Mehrere
// Anfragen desselben Clients innerhalb von ACCESS_LOG_COALESCE_MS gelten als
// ein Besuch (nur der Zeitstempel wird aktualisiert), damit ein einzelner
// Seitenaufruf nicht gleich mehrere Log-Zeilen erzeugt.
let accessLog = [];
const ACCESS_LOG_MAX = 20;
const ACCESS_LOG_COALESCE_MS = 30000;

function recordAccess(ip) {
    const now = Date.now();
    const last = accessLog[0];
    if (last && last.ip === ip && (now - last.time) < ACCESS_LOG_COALESCE_MS) {
        last.time = now;
        return;
    }
    accessLog.unshift({ time: now, ip });
    if (accessLog.length > ACCESS_LOG_MAX) accessLog.length = ACCESS_LOG_MAX;
}
let hardwareSnapshot = null;
const publishedDiskIds = new Set();
// Alle in der aktuellen Sitzung gebeamten NZBs (nicht nur die letzte) –
// nötig, damit bei Mehrfachauswahl immer die gerade aktiv herunterladende
// Datei angezeigt werden kann, auch wenn danach schon weitere gesendet
// wurden. Jeder Eintrag: { fileName, totalSizeBytes, fileCount, groups,
// poster, receivedAt, nzbget, deleted }
let nzbBatch = [];
let currentNzbInfo = null; // Alias auf den zuletzt hochgeladenen Batch-Eintrag
let lastMoveResults = null;
let lastMoveAt = null;
let nzbgetStatus = null; // Status des aktuell ANGEZEIGTEN Eintrags
let nzbgetReachable = false;
let totalDownloadsCount = 0; // Gesamtzahl aller gebeamten NZBs (aus aktionen.log)
const NZB_BATCH_MAX = 30;
// Merkt sich, ob die NZBGet-Warteschlange beim letzten Poll noch nicht leer
// war – nur bei einem Wechsel "war aktiv -> jetzt leer" wird der kurze
// "fertig"-Impuls an Home Assistant gesendet (statt bei jedem Poll erneut).
let queueWasActive = false;

// -------------------------------------------------------------------------
// Persistente Konfiguration: Quell-/Zielverzeichnis je Kategorie. Die drei
// Basis-Kategorien (Filme/Serien/Musik) sind immer vorhanden; darüber
// hinaus können beliebig viele weitere Kategorien in config.categories
// gespeichert sein (siehe /api/categories).
// -------------------------------------------------------------------------

function loadConfig() {
    let parsed = {};
    try {
        parsed = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf-8'));
    } catch (err) {
        parsed = {};
    }

    const cfg = {
        sources: (parsed.sources && typeof parsed.sources === 'object') ? parsed.sources : {},
        targets: (parsed.targets && typeof parsed.targets === 'object') ? parsed.targets : {},
        categories: Array.isArray(parsed.categories) ? parsed.categories : [],
        // Zweites (Ausweich-)Zielverzeichnis je Kategorie: wird genutzt, wenn
        // im ersten Ziel der Speicherplatz nicht reicht.
        altTargets: (parsed.altTargets && typeof parsed.altTargets === 'object') ? parsed.altTargets : {},
        // Ablageordner für Direkt-Uploads ("Beamen") – wenn nicht gesetzt,
        // wird der fest einprogrammierte UPLOAD_DIR als Standard genutzt.
        uploadDir: typeof parsed.uploadDir === 'string' && parsed.uploadDir ? parsed.uploadDir : null,
        // Erlaubte IP-Bereiche für den Zugriff auf das Tool (Network Settings).
        networkRanges: (Array.isArray(parsed.networkRanges) && parsed.networkRanges.length)
            ? parsed.networkRanges.filter((r) => typeof r === 'string' && r.trim())
            : DEFAULT_NETWORK_RANGES.slice(),
        // TMDB-API-Key für Filmcover (leer = Feature deaktiviert).
        tmdbApiKey: (typeof parsed.tmdbApiKey === 'string') ? parsed.tmdbApiKey.trim() : '',
    };

    // Sicherstellen, dass jede bekannte Kategorie (Basis + benutzerdefiniert)
    // einen Quell- und Ziel-Eintrag besitzt.
    BASE_CATEGORIES.concat(cfg.categories).forEach((c) => {
        if (!(c.key in cfg.sources)) cfg.sources[c.key] = null;
        if (!(c.key in cfg.targets)) cfg.targets[c.key] = null;
        if (!(c.key in cfg.altTargets)) cfg.altTargets[c.key] = null;
    });

    return cfg;
}

function saveConfig(cfg) {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2), 'utf-8');
}

let config = loadConfig();
let lastAccess = null;

// Aktuell aktiver Ablageordner für Direkt-Uploads: der konfigurierte
// Beam-Ablageordner, sonst der fest einprogrammierte Standard.
function getUploadDir() {
    return config.uploadDir || UPLOAD_DIR;
}

if (!fs.existsSync(getUploadDir())) {
    fs.mkdirSync(getUploadDir(), { recursive: true });
}

// -------------------------------------------------------------------------
// Server-Konsole: statisches Status-Dashboard statt endlos scrollender Logs.
// Der Bildschirm wird bei jeder Aktualisierung an derselben Stelle neu
// gezeichnet (wie bei "top"/"htop"), statt neue Zeilen anzuhängen.
// -------------------------------------------------------------------------

const MAX_LOG_LINES = 5;
let recentLogs = [];

function logEvent(message) {
    const ts = new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    recentLogs.push(`${ts}  ${message}`);
    if (recentLogs.length > MAX_LOG_LINES) recentLogs.shift();
    renderDashboard();
}

function dashRelTime(ms) {
    if (!ms) return 'noch nie';
    const diffSec = Math.round((Date.now() - ms) / 1000);
    if (diffSec < 60) return `vor ${diffSec}s`;
    const diffMin = Math.round(diffSec / 60);
    if (diffMin < 60) return `vor ${diffMin}min`;
    return `vor ${Math.round(diffMin / 60)}h`;
}

function dashDuration(sec) {
    if (sec === null || sec === undefined) return '–';
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    if (d) return `${d}d ${h}h`;
    if (h) return `${h}h ${m}min`;
    return `${m}min`;
}

function renderDashboard() {
    const lines = [];

    lines.push('NZB BEAMER');
    lines.push(`Port ${PORT}   MQTT: ${mqttClient && mqttClient.connected ? 'verbunden' : 'getrennt'}   Zugriff: ${dashRelTime(lastAccess)}${lastClientIp ? ' (' + lastClientIp + ')' : ''}`);
    lines.push('');

    if (hardwareSnapshot) {
        lines.push(`CPU ${hardwareSnapshot.cpu.loadPercent}%   RAM ${hardwareSnapshot.memory.usedPercent}%   Netz ↓${hardwareSnapshot.network.rxKBs}/↑${hardwareSnapshot.network.txKBs} kB/s   Uptime ${dashDuration(hardwareSnapshot.uptimeSeconds)}`);
    } else {
        lines.push('Hardware: noch keine Daten…');
    }
    lines.push('');

    getCategoryKeys().forEach((cat) => {
        lines.push(`${categoryLabel(cat)}: ${config.sources[cat] || '–'}  →  ${config.targets[cat] || '–'}`);
    });
    lines.push('');

    if (currentNzbInfo) {
        let nzbLine = `NZB: ${currentNzbInfo.fileName} (${bytesToGb(currentNzbInfo.totalSizeBytes)} GB)`;
        if (!nzbgetReachable) {
            nzbLine += ' – NZBGet nicht erreichbar';
        } else if (nzbgetStatus && nzbgetStatus.phase === 'active') {
            nzbLine += ` – ${nzbgetStatus.percent}% (${nzbgetStatus.downloadedMB}/${nzbgetStatus.totalMB} MB)`;
        } else if (nzbgetStatus && nzbgetStatus.phase === 'done') {
            nzbLine += ' – fertig';
        } else {
            nzbLine += ' – wartet auf NZBGet…';
        }
        lines.push(nzbLine);
    } else {
        lines.push('NZB: keine');
    }

    let moveLine = `Verschieben: ${dashRelTime(lastMoveAt)}`;
    if (lastMoveResults) {
        const summary = lastMoveResults
            .filter((r) => r.status === 'ok' || r.status === 'partial')
            .map((r) => `${categoryLabel(r.category)} ${r.moved}`)
            .join(', ');
        if (summary) moveLine += ` (${summary})`;
    }
    lines.push(moveLine);
    lines.push('');

    lines.push('Letzte Ereignisse:');
    (recentLogs.length ? recentLogs : ['(noch keine)']).forEach((l) => lines.push('  ' + l));

    process.stdout.write('\x1B[2J\x1B[H'); // Bildschirm leeren, Cursor an den Anfang
    process.stdout.write(lines.join('\n') + '\n');
}

setInterval(renderDashboard, 3000);

// -------------------------------------------------------------------------
// Aktions-Log: dauerhafte Protokollierung (u.a. NZB-Übertragungen) in einer
// Datei, eine JSON-Zeile pro Eintrag. Grundlage für den Dubletten-Check
// ("wurde diese Datei schon einmal gebeamt?").
// -------------------------------------------------------------------------

// fileName (normalisiert, siehe normalizeFileKey) -> { count, lastBeamedAt }
// Wird beim Start aus aktionen.log aufgebaut und bei jedem neuen Beam
// live nachgeführt – überlebt also Neustarts.
let beamedFileHistory = new Map();

function normalizeFileKey(fileName) {
    return String(fileName || '')
        .toLowerCase()
        .replace(/\.nzb$/i, '')
        .trim();
}

function recordBeamedFileHistory(fileName, timestamp) {
    const key = normalizeFileKey(fileName);
    if (!key) return;
    const existing = beamedFileHistory.get(key);
    beamedFileHistory.set(key, {
        count: (existing ? existing.count : 0) + 1,
        lastBeamedAt: timestamp,
    });
}

function logAction(entry) {
    const record = { timestamp: new Date().toISOString(), ...entry };
    fs.appendFile(ACTION_LOG_PATH, JSON.stringify(record) + '\n', (err) => {
        if (err) {
            logEvent('Aktions-Log konnte nicht geschrieben werden: ' + err.message);
            return;
        }
        if (entry.action === 'nzb_beamed') {
            totalDownloadsCount++;
            publishTotalDownloads();
            recordBeamedFileHistory(entry.fileName, record.timestamp);
        }
    });
    return record;
}

// Trägt ein erst NACH dem Schreiben aufgelöstes Poster nachträglich in genau
// diesen Protokoll-Eintrag ein (per Zeitstempel + Dateiname identifiziert).
// Damit kann der "nzb_beamed"-Eintrag sofort beim Upload geschrieben werden
// (neue Uploads tauchen dann direkt im "Zuletzt heruntergeladen"-Streifen
// und im Protokoll auf), ohne auf die TMDB-Antwort warten zu müssen – das
// Cover wird einfach ergänzt, sobald es da ist.
function patchActionLogPoster(timestamp, fileName, posterUrl) {
    if (!posterUrl) return;
    fs.readFile(ACTION_LOG_PATH, 'utf-8', (err, data) => {
        if (err) return;
        let changed = false;
        const lines = data.split('\n').filter(Boolean).map((line) => {
            if (changed) return line;
            let entry;
            try {
                entry = JSON.parse(line);
            } catch (e) {
                return line;
            }
            if (entry.action === 'nzb_beamed' && entry.timestamp === timestamp && entry.fileName === fileName && !entry.posterUrl) {
                entry.posterUrl = posterUrl;
                changed = true;
                return JSON.stringify(entry);
            }
            return line;
        });
        if (changed) {
            fs.writeFile(ACTION_LOG_PATH, lines.join('\n') + '\n', () => {});
        }
    });
}

// Zählt beim Start, wie viele NZBs insgesamt schon gebeamt wurden (für den
// "Gesamt gebeamte NZBs"-Sensor in Home Assistant, überlebt Neustarts), und
// baut gleichzeitig die Dubletten-Historie auf.
function initTotalDownloadsCount() {
    fs.readFile(ACTION_LOG_PATH, 'utf-8', (err, data) => {
        if (err) return;
        const beamedEntries = data
            .split('\n')
            .filter(Boolean)
            .map((line) => {
                try { return JSON.parse(line); } catch (e) { return null; }
            })
            .filter((entry) => entry && entry.action === 'nzb_beamed');

        totalDownloadsCount = beamedEntries.length;
        beamedFileHistory = new Map();
        beamedEntries.forEach((entry) => recordBeamedFileHistory(entry.fileName, entry.timestamp));

        publishTotalDownloads();
    });
}
initTotalDownloadsCount();

app.get('/api/log', (req, res) => {
    fs.readFile(ACTION_LOG_PATH, 'utf-8', (err, data) => {
        if (err) {
            if (err.code === 'ENOENT') return res.json({ entries: [] });
            return res.status(500).json({ error: err.message });
        }

        const limit = Math.min(500, parseInt(req.query.limit, 10) || 100);
        const entries = data
            .split('\n')
            .filter(Boolean)
            .map((line) => {
                try {
                    return JSON.parse(line);
                } catch (e) {
                    return null;
                }
            })
            .filter(Boolean)
            .reverse() // neueste zuerst
            .slice(0, limit);

        res.json({ entries });
    });
});

// Cover-Filmstreifen: liefert die letzten paar Beams MIT Poster. Ältere
// Protokoll-Einträge (von vor der TMDB-Anbindung bzw. von vor dieser
// Funktion) haben noch kein gespeichertes posterUrl-Feld – für die wird das
// Cover hier live nachgeholt (gleiche Cache-Logik wie beim NZBGet-Status),
// damit der Streifen nicht erst ab dem nächsten NEUEN Download etwas zeigt,
// sondern schon beim ersten Aufruf aus der vorhandenen Historie.
app.get('/api/recent-covers', async (req, res) => {
    const count = Math.min(20, Math.max(1, parseInt(req.query.count, 10) || 12));

    fs.readFile(ACTION_LOG_PATH, 'utf-8', async (err, data) => {
        if (err) return res.json({ covers: [] });

        const entries = data
            .split('\n')
            .filter(Boolean)
            .map((line) => { try { return JSON.parse(line); } catch (e) { return null; } })
            .filter((e) => e && e.action === 'nzb_beamed')
            .reverse() // neueste zuerst
            .slice(0, 50); // genug Kandidaten, um "count" mit Cover zu finden

        const covers = [];
        for (const e of entries) {
            if (covers.length >= count) break;
            let posterUrl = e.posterUrl || null;
            if (!posterUrl) {
                try {
                    const posterPath = await posterPromiseFor(e.fileName).promise;
                    posterUrl = posterImageUrl(posterPath, 'w154');
                } catch (err2) {
                    posterUrl = null;
                }
            }
            if (posterUrl) covers.push({ fileName: e.fileName, posterUrl, timestamp: e.timestamp });
        }

        res.json({ covers });
    });
});

// -------------------------------------------------------------------------
// IP-Filter: nur Zugriffe aus dem eigenen Intranet (192.168.178.x) sowie
// vom Server selbst (localhost) erlauben.
// -------------------------------------------------------------------------

function ipFilter(req, res, next) {
    let ip = req.ip || (req.connection && req.connection.remoteAddress) || '';
    if (ip.startsWith('::ffff:')) ip = ip.slice(7);

    if (isIpInRanges(ip, config.networkRanges)) {
        return next();
    }

    logEvent(`Zugriff blockiert von IP: ${ip}`);
    res.status(403).send('Zugriff verweigert: Diese IP ist nicht für den Zugriff freigegeben.');
}

app.use(ipFilter);

// "Letzter Zugriff" aktualisieren (Status-/Übersichts-Polling zählt nicht
// als echter Zugriff, sonst wäre der Zeitstempel bei offener Seite immer
// "jetzt").
app.use((req, res, next) => {
    const isPolling = req.method === 'GET' && ['/api/status', '/api/overview', '/api/hardware', '/api/nzb-status', '/api/move-status', '/api/nzbget-queue'].includes(req.path);
    if (!isPolling) {
        lastAccess = Date.now();
        let ip = req.ip || (req.connection && req.connection.remoteAddress) || '';
        if (ip.startsWith('::ffff:')) ip = ip.slice(7);
        lastClientIp = ip;
        recordAccess(ip);
        publishAccessInfo();
    }
    next();
});

app.use(cors());
app.use(express.json());
app.use(fileUpload());
app.use(express.static(__dirname));

// -------------------------------------------------------------------------
// NZB-Dateien auswerten (Gesamtgröße, Anzahl Dateien, Newsgroups)
// -------------------------------------------------------------------------

const nzbXmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });

// Erkennt Subject-Zeilen von Video-Dateien innerhalb einer NZB – deren
// Release-Name ist die zuverlässigste Grundlage für die Titel/Jahr-Erkennung
// (siehe extractTitleYear()), zuverlässiger als z.B. ein .par2-Eintrag.
const VIDEO_SUBJECT_RE = /\.(mkv|mp4|avi|ts|m2ts|m4v|wmv|mov)[\s"]/i;

function parseNzb(xmlContent) {
    const doc = nzbXmlParser.parse(xmlContent);
    const nzb = doc.nzb;
    if (!nzb || !nzb.file) {
        throw new Error('Keine gültige NZB-Struktur gefunden.');
    }

    const files = Array.isArray(nzb.file) ? nzb.file : [nzb.file];
    let totalSizeBytes = 0;
    const groupsSet = new Set();
    let poster = null;
    let releaseSubject = null;
    let fallbackSubject = null;

    files.forEach((f) => {
        if (poster === null && f['@_poster']) poster = f['@_poster'];

        const subject = f['@_subject'];
        if (subject) {
            if (fallbackSubject === null) fallbackSubject = subject;
            if (releaseSubject === null && VIDEO_SUBJECT_RE.test(subject)) releaseSubject = subject;
        }

        const segRaw = f.segments && f.segments.segment;
        if (segRaw) {
            const segments = Array.isArray(segRaw) ? segRaw : [segRaw];
            segments.forEach((s) => {
                const bytes = parseInt(s && s['@_bytes'], 10);
                if (!isNaN(bytes)) totalSizeBytes += bytes;
            });
        }

        const groupRaw = f.groups && f.groups.group;
        if (groupRaw) {
            const groups = Array.isArray(groupRaw) ? groupRaw : [groupRaw];
            groups.forEach((g) => groupsSet.add(typeof g === 'string' ? g : String(g)));
        }
    });

    return {
        fileCount: files.length,
        totalSizeBytes,
        groups: Array.from(groupsSet),
        poster,
        // Für die Titel/Jahr-Erkennung (Filmcover): bevorzugt das Subject
        // einer Video-Datei, sonst das erste verfügbare Subject.
        releaseSubject: releaseSubject || fallbackSubject,
    };
}

// -------------------------------------------------------------------------
// Filmcover (TMDB): Titel/Jahr aus einem Release-Namen erkennen, dazu passendes
// Poster bei The Movie Database suchen und als kleines Thumbnail anbieten.
// Komplett optional – ohne hinterlegten API-Key (config.tmdbApiKey) passiert
// hier gar nichts.
// -------------------------------------------------------------------------

// Typische Video-/Repair-Dateiendungen sowie Restzähler wie " (1/50)" oder
// führende "[12/34] - " / Anführungszeichen aus rohen Subject-/Dateinamen
// entfernen, bevor der eigentliche Release-Name übrig bleibt.
function extractTitleYear(rawName) {
    if (!rawName) return null;
    let name = String(rawName).trim();

    // Führendes "[12/34] - " (klassisches Subject-Präfix mancher Indexer).
    name = name.replace(/^\[\d+\/\d+\]\s*-\s*/, '');

    // Falls der Name in Anführungszeichen steht, nur den Inhalt nehmen.
    const quoted = name.match(/"([^"]+)"/);
    if (quoted) name = quoted[1];

    // Abschließenden Segmentzähler wie " (1/50)" entfernen.
    name = name.replace(/\s*\(\d+\/\d+\)\s*$/, '');

    // Bekannte Datei-/Repair-Endungen (inkl. mehrteiliger .volXXX+YYY.par2).
    name = name.replace(/\.vol\d+\+\d+\.par2$/i, '');
    name = name.replace(/\.(mkv|mp4|avi|ts|m2ts|m4v|wmv|mov|par2|nfo|rar|nzb|r\d{2,3}|\d{3})$/i, '');

    // Punkte/Unterstriche als Worttrenner behandeln.
    const normalized = name.replace(/[._]+/g, ' ').replace(/\s+/g, ' ').trim();
    if (!normalized) return null;

    const yearMatch = normalized.match(/\b(19\d{2}|20\d{2})\b/);
    if (!yearMatch) return null;

    const title = normalized.slice(0, yearMatch.index).trim();
    if (!title) return null;

    return { title, year: yearMatch[1] };
}

// TMDB gibt zwei verschiedene Schlüsseltypen aus, und die eigene Weboberfläche
// von TMDB bewirbt inzwischen bevorzugt den v4-Token – es ist also sehr
// wahrscheinlich, dass genau der (statt des kurzen v3-Keys) eingetragen wird:
//   - "API-Schlüssel (v3 auth)": kurzer 32-stelliger Hex-String, wird als
//     Query-Parameter "api_key=..." mitgeschickt.
//   - "API-Lesezugriffstoken (v4 auth)": langer JWT-artiger String (enthält
//     Punkte, i.d.R. weit über 40 Zeichen), muss als
//     "Authorization: Bearer <token>"-Header gesendet werden – als
//     api_key-Query-Parameter schlägt er mit HTTP 401 fehl.
// Damit beide Varianten ohne Rückfrage funktionieren, wird anhand der Form
// des gespeicherten Schlüssels automatisch der passende Auth-Modus gewählt.
function isTmdbV4Token(key) {
    return typeof key === 'string' && (key.length > 40 || key.includes('.'));
}

function tmdbSearchMoviePoster(title, year) {
    return new Promise((resolve) => {
        if (!config.tmdbApiKey) return resolve(null);

        const v4 = isTmdbV4Token(config.tmdbApiKey);
        const params = new URLSearchParams({
            query: title,
            language: 'de-DE',
        });
        if (!v4) params.set('api_key', config.tmdbApiKey);
        if (year) params.set('year', year);

        let settled = false;
        const safeResolve = (val) => { if (!settled) { settled = true; resolve(val); } };

        const url = `https://api.themoviedb.org/3/search/movie?${params.toString()}`;
        const options = { timeout: 6000 };
        if (v4) options.headers = { Authorization: `Bearer ${config.tmdbApiKey}`, Accept: 'application/json' };

        const req = https.get(url, options, (res) => {
            let raw = '';
            res.on('data', (chunk) => { raw += chunk; });
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    let detail = '';
                    try { detail = JSON.parse(raw).status_message || ''; } catch (err) { /* egal */ }
                    logEvent(`TMDB-Suche fehlgeschlagen (HTTP ${res.statusCode}${detail ? `: ${detail}` : ''}) für "${title}"${res.statusCode === 401 ? ' – TMDB API-Key prüfen (v3-Key statt v4-Lesezugriffstoken erforderlich, oder umgekehrt)' : ''}.`);
                    return safeResolve(null);
                }
                try {
                    const data = JSON.parse(raw);
                    const hit = data.results && data.results[0];
                    safeResolve(hit && hit.poster_path ? hit.poster_path : null);
                } catch (err) {
                    logEvent(`TMDB-Antwort konnte nicht gelesen werden: ${err.message}`);
                    safeResolve(null);
                }
            });
        });
        req.on('timeout', () => { req.destroy(); logEvent(`TMDB-Suche für "${title}" hat das Zeitlimit überschritten.`); safeResolve(null); });
        req.on('error', (err) => { logEvent(`TMDB-Suche für "${title}" fehlgeschlagen: ${err.message}`); safeResolve(null); });
    });
}

// Cache: "titel|jahr" -> { posterPath, ts, pending }. posterPath ist auch bei
// einem Nicht-Treffer gecacht (als null), damit dieselbe Anfrage nicht bei
// jedem Poll erneut an TMDB geschickt wird. Echte Treffer werden 24h lang
// gecacht, Nicht-Treffer/Fehler dagegen nur kurz – sonst würde z.B. ein
// falsch konfigurierter API-Key erst nach einem Tag erneut versucht.
const posterCache = new Map();
const POSTER_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const POSTER_NEGATIVE_CACHE_TTL_MS = 10 * 60 * 1000;

function posterImageUrl(posterPath, size) {
    return posterPath ? `https://image.tmdb.org/t/p/${size}${posterPath}` : null;
}

// Zentrale Cache-Logik: liefert sowohl den SOFORT verfügbaren (evtl. noch
// nicht ermittelten) Poster-Pfad als auch ein Promise, das auflöst sobald
// die TMDB-Suche fertig ist. Parallele Anfragen für denselben Titel teilen
// sich dieselbe laufende Anfrage (kein doppelter TMDB-Call).
function posterPromiseFor(releaseName) {
    if (!config.tmdbApiKey) return { immediate: null, promise: Promise.resolve(null) };
    const ty = extractTitleYear(releaseName);
    if (!ty) return { immediate: null, promise: Promise.resolve(null) };

    const key = `${ty.title.toLowerCase()}|${ty.year}`;
    const cached = posterCache.get(key);

    if (cached && !cached.pending) {
        const ttl = cached.posterPath ? POSTER_CACHE_TTL_MS : POSTER_NEGATIVE_CACHE_TTL_MS;
        if ((Date.now() - cached.ts) < ttl) {
            return { immediate: cached.posterPath, promise: Promise.resolve(cached.posterPath) };
        }
    }
    if (cached && cached.pending) {
        return { immediate: cached.posterPath, promise: cached.promise || Promise.resolve(cached.posterPath) };
    }

    const promise = tmdbSearchMoviePoster(ty.title, ty.year)
        .then((posterPath) => { posterCache.set(key, { posterPath, ts: Date.now(), pending: false }); return posterPath; })
        .catch(() => { posterCache.set(key, { posterPath: null, ts: Date.now(), pending: false }); return null; });
    posterCache.set(key, { posterPath: cached ? cached.posterPath : null, ts: cached ? cached.ts : 0, pending: true, promise });

    return { immediate: cached ? cached.posterPath : null, promise };
}

// Liefert SOFORT den zwischengespeicherten Poster-Pfad (oder null), ohne je
// auf TMDB zu warten – wichtig, da diese Funktion aus dem alle paar Sekunden
// gepollten /api/nzbget-queue-Endpunkt aufgerufen wird. Bei einem neuen bzw.
// abgelaufenen Cache-Eintrag wird im Hintergrund eine TMDB-Suche angestoßen;
// das Ergebnis steht dann erst beim übernächsten Poll zur Verfügung.
function posterPathFor(releaseName) {
    return posterPromiseFor(releaseName).immediate;
}

function buildNzbStatusPayload() {
    if (!currentNzbInfo) return null;

    return {
        fileName: currentNzbInfo.fileName,
        totalSizeBytes: currentNzbInfo.totalSizeBytes,
        totalSizeGb: bytesToGb(currentNzbInfo.totalSizeBytes),
        fileCount: currentNzbInfo.fileCount,
        groups: currentNzbInfo.groups,
        poster: currentNzbInfo.poster,
        receivedAt: currentNzbInfo.receivedAt,
        nzbget: nzbgetStatus,
        nzbgetReachable,
        // Ein Eintrag pro in dieser Charge gebeamter Datei, mit ihrem
        // jeweils eigenen Status (aktiv/wartet/fertig) – damit das Frontend
        // bei mehreren gleichzeitig gebeamten Dateien nicht nur die eine
        // "aktuelle" NZB zeigen kann, sondern die ganze Warteschlange der
        // eigenen Charge.
        batch: nzbBatch.map((it) => ({
            fileName: it.fileName,
            totalSizeGb: bytesToGb(it.totalSizeBytes),
            fileCount: it.fileCount,
            receivedAt: it.receivedAt,
            nzbget: it.nzbget || { phase: 'unknown' },
        })),
    };
}

app.get('/api/nzb-status', (req, res) => {
    res.json(buildNzbStatusPayload() || {});
});

// -------------------------------------------------------------------------
// NZBGet: echten Downloadfortschritt über die JSON-RPC-API abfragen
// -------------------------------------------------------------------------

function nzbgetCall(method, params = []) {
    return new Promise((resolve, reject) => {
        const body = JSON.stringify({ method, params, id: 1 });
        const auth = Buffer.from(`${NZBGET_USERNAME}:${NZBGET_PASSWORD}`).toString('base64');

        const req = http.request(
            {
                host: NZBGET_HOST,
                port: NZBGET_PORT,
                path: '/jsonrpc',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(body),
                    Authorization: `Basic ${auth}`,
                },
                timeout: 5000,
            },
            (res) => {
                let raw = '';
                res.on('data', (chunk) => { raw += chunk; });
                res.on('end', () => {
                    if (res.statusCode === 401) {
                        return reject(new Error('NZBGet hat die Zugangsdaten abgelehnt (401)'));
                    }
                    if (res.statusCode >= 400) {
                        return reject(new Error(`NZBGet antwortete mit HTTP ${res.statusCode}`));
                    }
                    try {
                        const data = JSON.parse(raw);
                        if (data.error) {
                            return reject(new Error(data.error.message || 'NZBGet-RPC-Fehler'));
                        }
                        resolve(data.result);
                    } catch (err) {
                        reject(new Error('Antwort von NZBGet konnte nicht gelesen werden: ' + err.message));
                    }
                });
            }
        );

        req.on('timeout', () => req.destroy(new Error('Zeitüberschreitung bei NZBGet-Anfrage')));
        req.on('error', (err) => reject(err));
        req.write(body);
        req.end();
    });
}

function matchesNzbName(candidate, targetLower) {
    if (!candidate) return false;
    const c = String(candidate).toLowerCase();
    return c === targetLower || c.includes(targetLower) || targetLower.includes(c);
}

// Ein Element der NZBGet-Warteschlange gilt als "gerade aktiv", wenn NZBGet
// es mit dem Status DOWNLOADING führt. Solche Elemente werden aus der
// Warteschlangen-Anzeige ausgeblendet, weil sie bereits prominent als
// aktiver Download in "Letzte NZB" gezeigt werden – doppelte Anzeige
// vermeiden.
function isActivelyDownloading(group) {
    return String((group && group.Status) || '').toUpperCase() === 'DOWNLOADING';
}

// Rechnet aus einem NZBGet-Warteschlangen-Element (listgroups-Eintrag) den
// Downloadfortschritt aus – gemeinsam genutzt von computeStatusForItem()
// (pro Batch-Eintrag) und /api/nzbget-queue (allgemeiner Status, unabhängig
// davon, ob die Datei über dieses Tool gebeamt wurde).
function groupProgress(group) {
    const totalMB = group.FileSizeMB || 0;
    const remainingMB = group.RemainingSizeMB || 0;
    const downloadedMB = group.DownloadedSizeMB != null
        ? group.DownloadedSizeMB
        : Math.max(0, totalMB - remainingMB);

    return {
        id: group.NZBID,
        status: group.Status,
        totalMB: round1(totalMB),
        downloadedMB: round1(downloadedMB),
        remainingMB: round1(remainingMB),
        percent: totalMB > 0 ? round1((downloadedMB / totalMB) * 100) : 0,
        health: group.Health,
    };
}

function computeStatusForItem(item, groups, history) {
    const targetName = item.fileName.replace(/\.nzb$/i, '').toLowerCase();

    const match = groups.find((g) => matchesNzbName(g.NZBName, targetName));
    if (match) {
        return { phase: 'active', ...groupProgress(match) };
    }

    const histMatch = history.find((h) => matchesNzbName(h.NZBName || h.Name, targetName));
    if (histMatch) {
        const totalMB = histMatch.FileSizeMB || 0;
        return {
            phase: 'done',
            status: histMatch.Status,
            totalMB: round1(totalMB),
            downloadedMB: round1(totalMB),
            remainingMB: 0,
            percent: 100,
            health: histMatch.Health,
        };
    }

    // Weder in Queue noch Historie – z.B. NZBGet hat die Datei noch nicht
    // eingelesen.
    return { phase: 'unknown' };
}

// Löscht eine fertig heruntergeladene NZB-Datei aus dem Direkt-Upload-
// Ordner, damit dort keine bereits verarbeiteten Dateien liegen bleiben.
function deleteProcessedNzbFile(item) {
    if (item.deleted) return;
    // Denselben Ordner verwenden, in den die Datei ursprünglich beim
    // Beamen gespeichert wurde (der Beam-Ablageordner kann sich seither
    // geändert haben).
    const filePath = path.join(item.uploadDir || getUploadDir(), item.fileName);
    fs.unlink(filePath, (err) => {
        if (err) {
            if (err.code !== 'ENOENT') {
                logEvent(`Verarbeitete NZB konnte nicht gelöscht werden (${item.fileName}): ${err.message}`);
            }
            return;
        }
        logEvent(`Verarbeitete NZB gelöscht: ${item.fileName}`);
    });
    item.deleted = true;
}

// Fragt den Status ALLER in dieser Sitzung gebeamten NZBs bei NZBGet ab
// (ein API-Aufruf für alle, nicht pro Datei) und zeigt im Frontend immer
// die gerade aktiv herunterladende NZB an – fällt auf die zuletzt
// gesendete zurück, wenn gerade keine aktiv ist. Ist ein Download fertig,
// wird die verarbeitete .nzb-Datei aus dem Upload-Ordner gelöscht.
async function updateNzbgetStatus() {
    try {
        // Warteschlange immer abfragen und an Home Assistant melden – auch
        // wenn wir selbst gerade noch nichts gebeamt haben (z.B. wenn
        // jemand einen Download direkt in NZBGet gestartet hat).
        const groups = await nzbgetCall('listgroups', [0]);
        nzbgetReachable = true;
        publishNzbgetReachable(true);
        const queueEmpty = publishNzbgetQueue(groups);

        if (nzbBatch.length === 0) {
            nzbgetStatus = null;
            currentNzbInfo = null;
            queueWasActive = !queueEmpty;
            publishNzbgetStatus();
            return;
        }

        const history = await nzbgetCall('history', [false]);

        let activeItem = null;

        // Von neu nach alt durchgehen, damit bei mehreren aktiven Downloads
        // der zuletzt gestartete angezeigt wird.
        for (let i = nzbBatch.length - 1; i >= 0; i--) {
            const item = nzbBatch[i];
            item.nzbget = computeStatusForItem(item, groups, history);

            if (item.nzbget.phase === 'done') {
                const justFinished = !item.deleted;
                deleteProcessedNzbFile(item);
                if (justFinished) {
                    // Diese Datei ist gerade fertig heruntergeladen worden –
                    // jetzt (statt erst beim nächsten Zeitintervall) alle
                    // Verzeichnisse auf verschiebbare Ordner/Dateien prüfen.
                    // Deckt sowohl "eine einzelne Datei fertig" als auch
                    // "die letzte Datei der Warteschlange fertig" ab.
                    runAutoMove();
                }
            }
            if (item.nzbget.phase === 'active' && !activeItem) {
                activeItem = item;
            }
        }

        // Wichtig: NICHT allein auf eine leere NZBGet-Warteschlange
        // (queueEmpty) abstellen – direkt nach dem Beamen ist die
        // Warteschlange oft noch kurz leer, weil NZBGet die Datei aus dem
        // Watch-Ordner erst einliest ("unknown"-Phase). Das würde die
        // gerade gebeamte Datei sofort wieder verschwinden lassen, statt
        // ihren Verarbeitungs-Status zu zeigen. Zurückgesetzt wird daher
        // erst, wenn WIRKLICH jeder Eintrag unseres eigenen Batches den
        // Status "done" erreicht hat.
        const allBatchDone = nzbBatch.every((it) => it.nzbget && it.nzbget.phase === 'done');

        if (allBatchDone) {
            // Alle von diesem Tool gestarteten Downloads sind fertig –
            // Anzeige komplett zurücksetzen, als wäre der Server gerade neu
            // gestartet: keine alten Dateinamen bleiben sichtbar.
            nzbBatch = [];
            currentNzbInfo = null;
            nzbgetStatus = null;
            publishNzbInfo(); // "aktuelle NZB" auch in Home Assistant zurücksetzen
        } else {
            // Anzeige: aktiver Download hat Vorrang, sonst die zuletzt
            // gesendete NZB (unabhängig von ihrem Status).
            const displayItem = activeItem || nzbBatch[nzbBatch.length - 1];
            currentNzbInfo = displayItem;
            nzbgetStatus = displayItem.nzbget;

            // Alte, bereits abgeschlossene Einträge am Anfang der Liste
            // entfernen, damit die Liste nicht unbegrenzt wächst.
            while (nzbBatch.length > NZB_BATCH_MAX && nzbBatch[0].nzbget && nzbBatch[0].nzbget.phase !== 'active') {
                nzbBatch.shift();
            }
        }

        queueWasActive = !queueEmpty;
    } catch (err) {
        nzbgetReachable = false;
        nzbgetStatus = null;
        publishNzbgetReachable(false);
        mqttClient && mqttClient.connected && mqttClient.publish(`${MQTT_BASE}/nzb/queue_count`, '0', { retain: true });
        publishAllDoneUnknown();
        logEvent('NZBGet nicht erreichbar: ' + err.message);
    }

    publishNzbgetStatus();
}

function publishNzbgetReachable(reachable) {
    if (!mqttClient || !mqttClient.connected) return;
    mqttClient.publish(`${MQTT_BASE}/nzb/nzbget_reachable`, reachable ? 'online' : 'offline', { retain: true });
}

// Meldet die Warteschlange an Home Assistant und gibt zurück, ob sie
// gerade komplett leer ist (auch wenn MQTT nicht verbunden ist – der
// Rückgabewert wird auch für den GUI-Reset in updateNzbgetStatus() benötigt).
function publishNzbgetQueue(groups) {
    const allGroups = groups || [];
    const queueEmpty = allGroups.length === 0;

    if (!mqttClient || !mqttClient.connected) return queueEmpty;

    // Aktiv herunterladende Elemente aus der Anzeige-Liste ausblenden – die
    // werden bereits prominent als "Letzte NZB" gezeigt, eine doppelte
    // Anzeige in der Warteschlange ist nur verwirrend.
    const items = allGroups
        .filter((g) => !isActivelyDownloading(g))
        .map((g) => ({
            name: g.NZBName || g.NZBFilename || 'unbekannt',
            size_mb: round1(g.FileSizeMB || 0),
        }));
    mqttClient.publish(`${MQTT_BASE}/nzb/queue_count`, String(items.length), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/nzb/queue_items`, JSON.stringify({ items }), { retain: true });

    // Kein Dauerzustand mehr, sondern ein kurzer Impuls: nur beim Wechsel
    // von "es lief noch etwas" zu "Warteschlange leer" einmal "done"
    // senden. Home Assistant setzt den binary_sensor über off_delay selbst
    // nach ein paar Sekunden wieder zurück (siehe publishNzbDiscovery()).
    if (queueEmpty && queueWasActive) {
        mqttClient.publish(`${MQTT_BASE}/nzb/all_done`, 'done', { retain: false });
    }

    return queueEmpty;
}

// Wird aufgerufen, wenn NZBGet nicht erreichbar ist – dann ist unklar, ob
// noch etwas läuft, daher keinen "fertig"-Impuls senden.
function publishAllDoneUnknown() {
    queueWasActive = false;
}

setTimeout(updateNzbgetStatus, 5000);
setInterval(updateNzbgetStatus, NZBGET_POLL_INTERVAL_MS);

// Prüft, ob eine Datei (nach Namen) schon einmal gebeamt wurde – Grundlage
// für die Rückfrage im Frontend, bevor eine bereits heruntergeladene Datei
// erneut übertragen wird.
app.get('/api/check-history', (req, res) => {
    const fileName = req.query.fileName;
    if (!fileName) {
        return res.status(400).json({ error: 'Kein Dateiname angegeben.' });
    }
    const info = beamedFileHistory.get(normalizeFileKey(fileName));
    if (!info) {
        return res.json({ seenBefore: false });
    }
    res.json({ seenBefore: true, count: info.count, lastBeamedAt: info.lastBeamedAt });
});

// Kompletter NZBGet-Status: die gerade aktiv herunterladende Datei (mit
// Fortschritt) plus die restliche Warteschlange – unabhängig davon, ob eine
// Datei über dieses Tool gebeamt oder direkt in NZBGet gestartet wurde.
// Bildet das "immer sichtbare" Status-Fenster im Frontend.
app.get('/api/nzbget-queue', async (req, res) => {
    try {
        const groups = await nzbgetCall('listgroups', [0]);
        const allGroups = groups || [];

        const activeGroup = allGroups.find((g) => isActivelyDownloading(g));
        const active = activeGroup ? {
            name: activeGroup.NZBName || activeGroup.NZBFilename || 'unbekannt',
            posterUrl: posterImageUrl(posterPathFor(activeGroup.NZBName || activeGroup.NZBFilename), 'w154'),
            ...groupProgress(activeGroup),
        } : null;

        // Aktiv herunterladendes Element aus der Warteliste ausblenden – das
        // wird bereits separat als "active" gezeigt, keine doppelte Anzeige.
        const items = allGroups
            .filter((g) => !isActivelyDownloading(g))
            .map((g) => ({
                id: g.NZBID,
                name: g.NZBName || g.NZBFilename || 'unbekannt',
                sizeMB: round1(g.FileSizeMB || 0),
                posterUrl: posterImageUrl(posterPathFor(g.NZBName || g.NZBFilename), 'w92'),
            }));

        // Über dieses Tool gebeamte Dateien, die noch nicht abgeschlossen
        // ("done") und noch nicht aktiv am Herunterladen sind (z.B. weil
        // NZBGet sie gerade erst aus dem Watch-Ordner einliest) – damit im
        // Status sichtbar bleibt, dass gerade etwas "verarbeitet" wird, bis
        // der eigentliche Download beginnt (dann übernimmt "active" oben).
        // Nutzt den von updateNzbgetStatus() ohnehin gepflegten Status je
        // Batch-Eintrag, statt NZBGet ein zweites Mal separat abzufragen.
        const processing = nzbBatch
            .filter((it) => {
                const phase = it.nzbget && it.nzbget.phase;
                if (phase === 'done') return false;
                if (phase === 'active' && it.nzbget.status === 'DOWNLOADING') return false;
                return true;
            })
            .map((it) => ({
                // Erst sobald NZBGet die Datei eingelesen hat, gibt es eine
                // NZBID zum Abbrechen – vorher (phase "unknown") ist id null
                // und der Stop-Button im Frontend entsprechend deaktiviert.
                id: (it.nzbget && it.nzbget.id) || null,
                name: it.fileName,
                // releaseSubject (aus der NZB selbst) ist zuverlässiger für die
                // Titel-Erkennung als der hochgeladene Dateiname.
                posterUrl: posterImageUrl(posterPathFor(it.releaseSubject || it.fileName), 'w154'),
            }));

        res.json({ active, items, processing, reachable: true });
    } catch (err) {
        res.json({ active: null, items: [], processing: [], reachable: false, error: err.message });
    }
});

// -------------------------------------------------------------------------
// Downloads abbrechen: einzelnen Eintrag oder die komplette Warteschlange
// (inkl. aktivem Download) löschen. Nutzt NZBGets editqueue/"GroupDelete",
// das sowohl aktive als auch wartende Einträge aus der Queue entfernt.
// -------------------------------------------------------------------------

app.post('/api/nzbget-cancel', async (req, res) => {
    const id = req.body && req.body.id;
    if (!id) {
        return res.status(400).json({ error: 'Keine NZBID angegeben.' });
    }
    try {
        await nzbgetCall('editqueue', ['GroupDelete', 0, '', [id]]);
        logEvent(`Download abgebrochen (NZBID ${id}).`);
        setTimeout(updateNzbgetStatus, 1500);
        res.json({ ok: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/nzbget-cancel-all', async (req, res) => {
    try {
        const groups = await nzbgetCall('listgroups', [0]);
        const ids = (groups || []).map((g) => g.NZBID).filter((id) => id != null);

        if (ids.length) {
            await nzbgetCall('editqueue', ['GroupDelete', 0, '', ids]);
        }

        // Eigene Anzeige ebenfalls sofort zurücksetzen, statt auf den
        // nächsten Poll zu warten (der ohnehin dasselbe feststellen würde).
        nzbBatch = [];
        currentNzbInfo = null;
        nzbgetStatus = null;
        publishNzbInfo();

        logEvent(`Alle Downloads abgebrochen (${ids.length} Eintrag/Einträge, inkl. Warteschlange).`);
        setTimeout(updateNzbgetStatus, 1500);
        res.json({ ok: true, count: ids.length });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// -------------------------------------------------------------------------
// Direkt-Upload: Datei vom Sender-Browser hochladen (plus NZB-Auswertung)
// -------------------------------------------------------------------------

app.post('/upload', (req, res) => {
    if (!req.files || !req.files.nzbFile) {
        return res.status(400).send('Keine Datei hochgeladen.');
    }

    const file = req.files.nzbFile;

    if (!file.name.endsWith('.nzb')) {
        return res.status(400).send('Nur .nzb-Dateien sind erlaubt!');
    }

    // NZB-Inhalt auswerten, BEVOR die Datei verschoben wird – file.data
    // enthält den Inhalt noch im Speicher.
    const activeUploadDir = getUploadDir();
    try {
        const parsed = parseNzb(file.data.toString('utf-8'));
        const nzbEntry = {
            fileName: file.name,
            totalSizeBytes: parsed.totalSizeBytes,
            fileCount: parsed.fileCount,
            groups: parsed.groups,
            poster: parsed.poster,
            releaseSubject: parsed.releaseSubject,
            receivedAt: Date.now(),
            nzbget: null,
            deleted: false,
            uploadDir: activeUploadDir,
        };
        // Falls dieselbe Datei erneut gesendet wird, alten Eintrag ersetzen
        // statt zu duplizieren.
        const existingIdx = nzbBatch.findIndex((it) => it.fileName === file.name);
        if (existingIdx !== -1) nzbBatch.splice(existingIdx, 1);
        nzbBatch.push(nzbEntry);
        currentNzbInfo = nzbEntry; // sofortige Anzeige, bis der nächste Status-Poll übernimmt

        logEvent(`NZB ausgewertet: ${file.name} – ${parsed.fileCount} Datei(en), ${bytesToGb(parsed.totalSizeBytes)} GB`);
        publishNzbInfo();
        // Protokoll-Eintrag wird SOFORT geschrieben (nicht erst nach der
        // TMDB-Antwort) – sonst fehlt ein gerade gebeamter Upload für ein
        // paar Sekunden im "Zuletzt heruntergeladen"-Streifen und im
        // Protokoll. Das Poster wird, sobald TMDB geantwortet hat, per
        // patchActionLogPoster() nachträglich in genau diesen Eintrag
        // eingetragen; bis dahin greift bei Bedarf die Live-Nachladung in
        // /api/recent-covers.
        const beamedRecord = logAction({
            action: 'nzb_beamed',
            ip: lastClientIp,
            fileName: file.name,
            totalSizeBytes: parsed.totalSizeBytes,
            totalSizeGb: bytesToGb(parsed.totalSizeBytes),
            fileCount: parsed.fileCount,
            groups: parsed.groups,
            posterUrl: null,
        });
        posterPromiseFor(parsed.releaseSubject || file.name).promise.then((posterPath) => {
            const posterUrl = posterImageUrl(posterPath, 'w154');
            if (posterUrl) patchActionLogPoster(beamedRecord.timestamp, file.name, posterUrl);
        }).catch(() => {});
        nzbgetStatus = null;
        // NZBGet braucht nach dem Speichern der Datei im Watch-Ordner einen
        // Moment, um sie einzulesen – daher mit kurzer Verzögerung prüfen.
        setTimeout(updateNzbgetStatus, 4000);
    } catch (err) {
        logEvent('NZB konnte nicht ausgewertet werden: ' + err.message);
    }

    if (!fs.existsSync(activeUploadDir)) {
        try {
            fs.mkdirSync(activeUploadDir, { recursive: true });
        } catch (err) {
            logEvent('Beam-Ablageordner konnte nicht erstellt werden: ' + err.message);
            return res.status(500).send('Beam-Ablageordner konnte nicht erstellt werden.');
        }
    }

    const savePath = path.join(activeUploadDir, file.name);

    file.mv(savePath, (err) => {
        if (err) {
            logEvent('Fehler beim Speichern: ' + err.message);
            return res.status(500).send('Fehler beim Speichern der Datei.');
        }
        logEvent(`Datei gespeichert: ${savePath}`);
        res.send(`Datei "${file.name}" erfolgreich übertragen!`);
    });
});

// -------------------------------------------------------------------------
// Laufwerke ermitteln (Windows: Buchstaben A–Z durchprobieren)
// -------------------------------------------------------------------------

app.get('/api/drives', (req, res) => {
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
    const drives = [];

    for (const letter of letters) {
        const drivePath = `${letter}:\\`;
        try {
            fs.accessSync(drivePath, fs.constants.R_OK);
            drives.push(drivePath);
        } catch (err) {
            // Laufwerk nicht vorhanden / nicht zugreifbar
        }
    }

    res.json({ drives });
});

// -------------------------------------------------------------------------
// Ordner eines Pfads auflisten (für den Ordner-Browser im Frontend)
// -------------------------------------------------------------------------

app.get('/api/browse', (req, res) => {
    const dirPath = req.query.path;
    if (!dirPath) {
        return res.status(400).json({ error: 'Kein Pfad angegeben.' });
    }

    try {
        const entries = fs.readdirSync(dirPath, { withFileTypes: true });
        const folders = entries
            .filter((e) => e.isDirectory())
            .map((e) => e.name)
            .sort((a, b) => a.localeCompare(b, 'de'));

        const parent = path.dirname(dirPath);
        const hasParent = parent && parent !== dirPath;

        res.json({
            path: dirPath,
            parent: hasParent ? parent : null,
            folders,
        });
    } catch (err) {
        res.status(500).json({ error: `Ordner konnte nicht gelesen werden: ${err.message}` });
    }
});

// -------------------------------------------------------------------------
// Konfiguration lesen / einzelnes Verzeichnis setzen
// -------------------------------------------------------------------------

app.get('/api/config', (req, res) => {
    res.json(config);
});

app.post('/api/config', (req, res) => {
    const { section, category, path: dirPath } = req.body || {};

    if (!['source', 'target', 'target2'].includes(section) || !getCategoryKeys().includes(category) || !dirPath) {
        return res.status(400).json({ error: 'Ungültige Anfrage: section (source/target), category (filme/serien/musik) und path erforderlich.' });
    }

    if (section === 'source') config.sources[category] = dirPath;
    if (section === 'target') config.targets[category] = dirPath;
    if (section === 'target2') config.altTargets[category] = dirPath;

    saveConfig(config);
    publishOverview();
    res.json(config);
});

app.post('/api/config/clear', (req, res) => {
    const { section, category } = req.body || {};

    if (!['source', 'target', 'target2'].includes(section) || !getCategoryKeys().includes(category)) {
        return res.status(400).json({ error: 'Ungültige Anfrage: section (source/target) und category (filme/serien/musik) erforderlich.' });
    }

    if (section === 'source') config.sources[category] = null;
    if (section === 'target') config.targets[category] = null;
    if (section === 'target2') config.altTargets[category] = null;

    saveConfig(config);
    publishOverview();
    res.json(config);
});

// -------------------------------------------------------------------------
// Beam-Ablageordner: wo Direkt-Upload-Dateien (per "Jetzt Beamen") auf
// diesem Server gespeichert werden. Standard ist UPLOAD_DIR, kann aber
// über die Quellverzeichnisse-Ansicht überschrieben werden.
// -------------------------------------------------------------------------

app.post('/api/upload-dir', (req, res) => {
    const dirPath = req.body && req.body.path;
    if (!dirPath) {
        return res.status(400).json({ error: 'Kein Pfad angegeben.' });
    }
    config.uploadDir = dirPath;
    saveConfig(config);
    if (!fs.existsSync(dirPath)) {
        try { fs.mkdirSync(dirPath, { recursive: true }); } catch (err) { /* wird beim nächsten Upload erneut versucht */ }
    }
    logEvent(`Beam-Ablageordner geändert: ${dirPath}`);
    res.json({ uploadDir: getUploadDir(), config });
});

app.post('/api/upload-dir/clear', (req, res) => {
    config.uploadDir = null;
    saveConfig(config);
    logEvent(`Beam-Ablageordner auf Standard zurückgesetzt: ${getUploadDir()}`);
    res.json({ uploadDir: getUploadDir(), config });
});

// -------------------------------------------------------------------------
// Network Settings: erlaubte IP-Bereiche für den Zugriff auf das Tool.
// Ein Eintrag, der mit einem Punkt endet, wirkt als Präfix (z.B.
// "192.168.178." erlaubt 192.168.178.x), alles andere als exakte IP.
// localhost ist immer erlaubt und muss nicht extra aufgeführt werden.
// -------------------------------------------------------------------------

app.get('/api/network-ranges', (req, res) => {
    res.json({ ranges: config.networkRanges || [] });
});

app.post('/api/network-ranges', (req, res) => {
    const ranges = req.body && req.body.ranges;
    if (!Array.isArray(ranges) || ranges.some((r) => typeof r !== 'string')) {
        return res.status(400).json({ error: 'Ungültige Liste von IP-Bereichen.' });
    }
    const cleaned = [...new Set(ranges.map((r) => r.trim()).filter(Boolean))];

    // Selbst-Aussperren verhindern: die anfragende IP muss nach der Änderung
    // weiterhin Zugriff haben (localhost ist ohnehin immer erlaubt).
    let requestIp = req.ip || (req.connection && req.connection.remoteAddress) || '';
    if (requestIp.startsWith('::ffff:')) requestIp = requestIp.slice(7);
    if (!isIpInRanges(requestIp, cleaned)) {
        return res.status(400).json({ error: `Diese Änderung würde deinen eigenen Zugriff sperren (${requestIp} wäre nicht mehr erlaubt). Bitte diese IP mit aufnehmen.` });
    }

    config.networkRanges = cleaned;
    saveConfig(config);
    logEvent(`Erlaubte IP-Bereiche geändert: ${cleaned.join(', ')}`);
    res.json({ ranges: config.networkRanges });
});

app.post('/api/network-ranges/reset', (req, res) => {
    config.networkRanges = DEFAULT_NETWORK_RANGES.slice();
    saveConfig(config);
    logEvent(`Erlaubte IP-Bereiche auf Standard zurückgesetzt: ${config.networkRanges.join(', ')}`);
    res.json({ ranges: config.networkRanges });
});

// -------------------------------------------------------------------------
// TMDB-API-Key für Filmcover (Einstellungen-Modal).
// -------------------------------------------------------------------------

app.get('/api/tmdb-key', (req, res) => {
    res.json({ key: config.tmdbApiKey || '' });
});

app.post('/api/tmdb-key', (req, res) => {
    const key = (req.body && typeof req.body.key === 'string') ? req.body.key.trim() : '';
    config.tmdbApiKey = key;
    saveConfig(config);
    // Cache leeren, damit ein neu eingegebener Key sofort greift (bzw. ein
    // entfernter Key nicht versehentlich alte Treffer weiterzeigt).
    posterCache.clear();
    logEvent(key ? 'TMDB API-Key gespeichert – Filmcover aktiviert.' : 'TMDB API-Key entfernt – Filmcover deaktiviert.');
    res.json({ key: config.tmdbApiKey });
});

// -------------------------------------------------------------------------
// Kategorien: neben den 3 Basis-Kategorien (Filme/Serien/Musik) können
// beliebig weitere angelegt werden. Jede neue Kategorie erhält automatisch
// sowohl einen Quell- als auch einen Ziel-Verzeichnis-Platz (beide Reiter
// zeigen dieselbe Kategorienliste).
// -------------------------------------------------------------------------

app.get('/api/categories', (req, res) => {
    res.json({ categories: getCategories() });
});

app.post('/api/categories', (req, res) => {
    const label = (req.body && String(req.body.label || '').trim());
    if (!label) {
        return res.status(400).json({ error: 'Bitte einen Namen für die neue Kategorie angeben.' });
    }
    if (label.length > 40) {
        return res.status(400).json({ error: 'Name ist zu lang (max. 40 Zeichen).' });
    }
    if (getCategories().some((c) => c.label.toLowerCase() === label.toLowerCase())) {
        return res.status(400).json({ error: `Eine Kategorie "${label}" gibt es bereits.` });
    }

    const key = slugifyCategoryKey(label);
    const newCat = { key, label, mdiIcon: 'mdi:folder-outline', emoji: '📁' };

    config.categories.push(newCat);
    config.sources[key] = null;
    config.targets[key] = null;
    saveConfig(config);

    // Sofort bei Home Assistant registrieren, statt auf den nächsten
    // Neustart/Reconnect zu warten.
    publishCategoryDiscovery(key);
    publishOverview();

    logEvent(`Neue Kategorie angelegt: ${label} (Quelle + Ziel)`);
    res.json({ categories: getCategories(), config });
});

// -------------------------------------------------------------------------
// Übersicht: Inhalt aller 6 konfigurierten Verzeichnisse (3 Quellen,
// 3 Ziele)
// -------------------------------------------------------------------------

function listDirFiles(dirPath) {
    if (!dirPath) return { files: [], error: null };
    try {
        const entries = fs.readdirSync(dirPath, { withFileTypes: true });
        const items = entries
            .map((e) => {
                const full = path.join(dirPath, e.name);
                if (e.isDirectory()) {
                    let mtime = 0;
                    try { mtime = fs.statSync(full).mtimeMs; } catch (err) { /* ignore */ }
                    return { name: e.name, type: 'dir', size: null, mtime };
                }
                try {
                    const stat = fs.statSync(full);
                    return { name: e.name, type: 'file', size: stat.size, mtime: stat.mtimeMs };
                } catch (err) {
                    return { name: e.name, type: 'file', size: 0, mtime: 0 };
                }
            })
            .sort((a, b) => {
                if (a.type !== b.type) return a.type === 'dir' ? -1 : 1; // Ordner zuerst
                return a.name.localeCompare(b.name, 'de');
            });
        return { files: items, error: null };
    } catch (err) {
        return { files: [], error: err.message };
    }
}

app.get('/api/overview', (req, res) => {
    const overview = { sources: {}, targets: {}, altTargets: {} };

    getCategoryKeys().forEach((c) => {
        overview.sources[c] = { dir: config.sources[c], ...listDirFiles(config.sources[c]) };
        overview.targets[c] = { dir: config.targets[c], ...listDirFiles(config.targets[c]) };
        overview.altTargets[c] = { dir: config.altTargets[c], ...listDirFiles(config.altTargets[c]) };
    });

    res.json(overview);
});

// -------------------------------------------------------------------------
// Leere Ordner in den Zielverzeichnissen finden und auf Wunsch löschen
// (z.B. Reste, die nach dem Verschieben/Entpacken übrig bleiben).
// -------------------------------------------------------------------------

// Durchsucht "dir" rekursiv nach Unterordnern, die komplett leer sind
// (0 Einträge), und sammelt sie in "results". Von unten nach oben (erst die
// Unterordner), damit ein Ordner, der nur leere Unterordner enthält, nach
// deren Löschung beim nächsten Lauf ebenfalls gefunden wird.
function collectEmptyDirs(dir, categoryKey, results) {
    let entries;
    try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (err) {
        return;
    }
    entries.filter((e) => e.isDirectory()).forEach((e) => {
        collectEmptyDirs(path.join(dir, e.name), categoryKey, results);
    });
    if (entries.length === 0) {
        results.push({ category: categoryKey, path: dir });
    }
}

app.get('/api/empty-dirs', (req, res) => {
    const results = [];
    // Über ALLE definierten Zielverzeichnisse gehen – nicht nur die
    // aktuell bekannten Kategorien, sondern jeden Eintrag in
    // config.targets, der einen Pfad gesetzt hat.
    const allTargets = [];
    Object.keys(config.targets).forEach((cat) => {
        allTargets.push([cat, config.targets[cat]]);
        if (config.altTargets && config.altTargets[cat]) allTargets.push([cat, config.altTargets[cat]]);
    });
    allTargets.forEach(([cat, targetDir]) => {
        if (!targetDir || !fs.existsSync(targetDir)) return;
        let entries;
        try {
            entries = fs.readdirSync(targetDir, { withFileTypes: true });
        } catch (err) {
            return;
        }
        entries.filter((e) => e.isDirectory()).forEach((e) => {
            collectEmptyDirs(path.join(targetDir, e.name), cat, results);
        });
    });
    res.json({ items: results });
});

app.post('/api/empty-dirs/delete', (req, res) => {
    const { paths: dirPaths } = req.body || {};
    if (!Array.isArray(dirPaths) || dirPaths.length === 0) {
        return res.status(400).json({ error: 'Keine Verzeichnisse angegeben.' });
    }

    // Sicherheit: nur Pfade löschen, die tatsächlich unterhalb eines
    // konfigurierten Zielverzeichnisses liegen.
    const targetRoots = Object.values(config.targets).concat(Object.values(config.altTargets || {})).filter(Boolean).map((d) => path.resolve(d));

    const deleted = [];
    const failed = [];

    dirPaths.forEach((p) => {
        const resolved = path.resolve(p);
        const insideTarget = targetRoots.some((t) => resolved === t || resolved.startsWith(t + path.sep));
        if (!insideTarget) {
            failed.push({ path: p, reason: 'Liegt außerhalb der Zielverzeichnisse.' });
            return;
        }
        try {
            const stillEmpty = fs.readdirSync(resolved).length === 0;
            if (!stillEmpty) {
                failed.push({ path: p, reason: 'Ist inzwischen nicht mehr leer.' });
                return;
            }
            fs.rmdirSync(resolved);
            deleted.push(p);
        } catch (err) {
            failed.push({ path: p, reason: err.message });
        }
    });

    if (deleted.length) {
        logEvent(`Leere Ordner gelöscht: ${deleted.length}${failed.length ? `, ${failed.length} fehlgeschlagen` : ''}`);
        publishOverview();
    }

    res.json({ deleted, failed });
});

// -------------------------------------------------------------------------
// Alle Daten verschieben: pro Kategorie alle Dateien vom Quell- ins
// Zielverzeichnis verschieben
// -------------------------------------------------------------------------

const fsp = fs.promises;

function uniqueDestPath(destDir, name) {
    let candidate = path.join(destDir, name);
    if (!fs.existsSync(candidate)) return candidate;

    const ext = path.extname(name);
    const base = path.basename(name, ext);
    let i = 1;
    let alt;
    do {
        alt = path.join(destDir, `${base} (${i})${ext}`);
        i++;
    } while (fs.existsSync(alt));
    return alt;
}

// Verschiebt EIN Element (Ordner oder Datei) komplett vom Quell- ins
// Zielverzeichnis. Ordner werden als Ganzes verschoben (inkl. Inhalt),
// keine rekursive Datei-Suche nötig – die Ordner selbst sind die Einheit.
async function moveOneItem(srcFull, destDir, isDir) {
    const name = path.basename(srcFull);
    const destPath = uniqueDestPath(destDir, name);

    try {
        await fsp.rename(srcFull, destPath);
    } catch (err) {
        if (err.code === 'EXDEV') {
            // Anderes Laufwerk: kopieren (bei Ordnern rekursiv) und danach
            // das Original entfernen.
            await fsp.cp(srcFull, destPath, { recursive: isDir });
            await fsp.rm(srcFull, { recursive: isDir, force: true });
        } else {
            throw err;
        }
    }
    return destPath;
}

// Laufwerk (Root) eines Pfads, z.B. "d:\" – dient zur Erkennung, ob ein
// Verschieben nur umbenennt (gleiches Laufwerk, kein Zusatzplatz nötig) oder
// kopieren muss (anderes Laufwerk, Platz im Ziel nötig).
function driveRootOf(p) {
    return path.parse(path.resolve(p)).root.toLowerCase();
}

// Gesamtgröße eines Elements (Datei oder Ordner rekursiv) in Bytes.
async function itemSizeBytes(fullPath) {
    let st;
    try {
        st = await fsp.lstat(fullPath);
    } catch (e) {
        return 0;
    }
    if (!st.isDirectory()) return st.size;
    let total = 0;
    let children = [];
    try {
        children = await fsp.readdir(fullPath);
    } catch (e) {
        return 0;
    }
    for (const child of children) {
        total += await itemSizeBytes(path.join(fullPath, child));
    }
    return total;
}

// Prüft vor dem Verschieben, ob im Zielverzeichnis genug freier Speicher
// vorhanden ist. Nur nötig, wenn Quelle und Ziel auf verschiedenen Laufwerken
// liegen (dann wird kopiert + gelöscht). Liefert { ok, message }.
async function checkFreeSpaceForMove(sourceDir, targetDir, entries) {
    if (driveRootOf(sourceDir) === driveRootOf(targetDir)) {
        return { ok: true, message: null };
    }
    let needed = 0;
    for (const entry of entries) {
        needed += await itemSizeBytes(path.join(sourceDir, entry.name));
    }
    if (needed === 0) return { ok: true, message: null };

    let free;
    try {
        const st = await fsp.statfs(targetDir);
        free = Number(st.bavail) * Number(st.bsize);
    } catch (err) {
        return {
            ok: false,
            message: `Freier Speicher im Zielverzeichnis konnte nicht geprüft werden (${err.message}).`,
        };
    }

    const safetyMargin = 512 * 1024 * 1024; // 0,5 GB Reserve
    if (free < needed + safetyMargin) {
        return {
            ok: false,
            message: `Nicht genug Speicherplatz im Ziel ${targetDir}: benötigt ${bytesToGb(needed)} GB (plus Reserve), frei ${bytesToGb(free)} GB.`,
        };
    }
    return { ok: true, message: null };
}

// Führt den kompletten Verschiebe-Lauf aus und meldet jeden Schritt über
// onProgress (für die Live-Anzeige im Browser bzw. Konsolen-Logging).
async function performMoveAll(onProgress) {
    const notify = typeof onProgress === 'function' ? onProgress : () => {};
    const results = [];

    for (const category of getCategoryKeys()) {
        const sourceDir = config.sources[category];
        let targetDir = config.targets[category];
        const altTargetDir = config.altTargets ? config.altTargets[category] : null;
        let switchNotice = null;

        if (!sourceDir || !targetDir) {
            results.push({ category, status: 'skipped', reason: 'Quell- oder Zielverzeichnis nicht gesetzt', moved: 0 });
            notify({ category, item: null, status: 'skipped', message: 'Quell- oder Zielverzeichnis nicht gesetzt' });
            continue;
        }

        notify({ category, item: null, status: 'category-start', message: `Prüfe ${categoryLabel(category)}…` });

        if (!fs.existsSync(sourceDir)) {
            const msg = `Quellverzeichnis existiert nicht: ${sourceDir}`;
            results.push({ category, status: 'error', reason: msg, moved: 0 });
            notify({ category, item: null, status: 'error', message: msg });
            continue;
        }

        if (!fs.existsSync(targetDir)) {
            try {
                fs.mkdirSync(targetDir, { recursive: true });
            } catch (err) {
                const msg = `Zielverzeichnis konnte nicht erstellt werden: ${err.message}`;
                results.push({ category, status: 'error', reason: msg, moved: 0 });
                notify({ category, item: null, status: 'error', message: msg });
                continue;
            }
        }

        let entries;
        try {
            entries = fs.readdirSync(sourceDir, { withFileTypes: true });
        } catch (err) {
            const msg = `Quellverzeichnis konnte nicht gelesen werden: ${err.message}`;
            results.push({ category, status: 'error', reason: msg, moved: 0 });
            notify({ category, item: null, status: 'error', message: msg });
            continue;
        }

        if (entries.length > 0) {
            notify({ category, item: null, status: 'category-start', message: `Prüfe Speicherplatz für ${categoryLabel(category)}…` });
            const space = await checkFreeSpaceForMove(sourceDir, targetDir, entries);
            if (!space.ok) {
                let resolved = false;
                let failMsg = space.message;
                if (altTargetDir) {
                    try {
                        if (!fs.existsSync(altTargetDir)) fs.mkdirSync(altTargetDir, { recursive: true });
                        const altSpace = await checkFreeSpaceForMove(sourceDir, altTargetDir, entries);
                        if (altSpace.ok) {
                            switchNotice = `Speicherplatz knapp: ${space.message} Es wurde auf das zweite Zielverzeichnis ${altTargetDir} gewechselt.`;
                            targetDir = altTargetDir;
                            resolved = true;
                        } else {
                            failMsg = `${space.message} Auch das zweite Ziel reicht nicht: ${altSpace.message}`;
                        }
                    } catch (err) {
                        failMsg = `${space.message} Zweites Ziel nicht nutzbar: ${err.message}`;
                    }
                }
                if (!resolved) {
                    failMsg += ' Verschieben nicht durchgeführt.';
                    results.push({ category, status: 'error', reason: failMsg, moved: 0 });
                    notify({ category, item: null, status: 'error', message: failMsg });
                    logEvent(`${categoryLabel(category)}: ${failMsg}`);
                    continue;
                }
                notify({ category, item: null, status: 'category-start', message: switchNotice });
                logEvent(`${categoryLabel(category)}: ${switchNotice}`);
            }
        }

        let moved = 0;
        let failed = 0;
        let lastError = null;

        for (const entry of entries) {
            const srcFull = path.join(sourceDir, entry.name);
            const isDir = entry.isDirectory();

            notify({ category, item: entry.name, status: 'item-start', message: `${isDir ? 'Ordner' : 'Datei'} "${entry.name}" wird verschoben…` });

            try {
                await moveOneItem(srcFull, targetDir, isDir);
                moved++;
                notify({ category, item: entry.name, status: 'item-done', message: `"${entry.name}" verschoben.` });
            } catch (err) {
                failed++;
                lastError = err.message;
                notify({ category, item: entry.name, status: 'item-error', message: `"${entry.name}" fehlgeschlagen: ${err.message}` });
            }
        }

        if (failed > 0) {
            results.push({ category, status: 'partial', reason: `${failed} Element(e) fehlgeschlagen (${lastError})`, notice: switchNotice, moved });
        } else if (moved === 0) {
            results.push({ category, status: 'empty', reason: 'Keine Ordner/Dateien im Quellverzeichnis gefunden', moved: 0 });
        } else {
            results.push({ category, status: 'ok', reason: null, notice: switchNotice, moved });
        }

        notify({ category, item: null, status: 'category-done', message: `${categoryLabel(category)}: ${moved} verschoben${failed ? `, ${failed} fehlgeschlagen` : ''}.` });
    }

    return results;
}

// Status des letzten Verschiebe-Laufs (automatisch oder manuell).
app.get('/api/move-status', (req, res) => {
    res.json({ results: lastMoveResults, lastRunAt: lastMoveAt });
});

// Manuelles Anstoßen eines Verschiebe-Laufs (Button im Frontend, siehe
// "Letztes automatisches Verschieben"-Box). Nutzt dieselbe Logik wie der
// automatische Hintergrund-Lauf (siehe runAutoMove/executeMoveAll weiter
// unten) und verhindert, dass zwei Läufe gleichzeitig laufen.
app.post('/api/move-now', async (req, res) => {
    if (autoMoveRunning) {
        return res.status(409).json({ error: 'Es läuft bereits ein Verschiebe-Vorgang.' });
    }
    try {
        const results = await executeMoveAll('manuell');
        res.json({ results, lastRunAt: lastMoveAt });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// -------------------------------------------------------------------------
// Serverinfo: eigene LAN-IP, Port, letzter Zugriff
// -------------------------------------------------------------------------

function getServerIp() {
    const interfaces = os.networkInterfaces();
    let fallback = null;

    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                const prefixRanges = (config.networkRanges || []).filter((r) => r.endsWith('.'));
                if (prefixRanges.some((p) => iface.address.startsWith(p))) return iface.address;
                if (!fallback) fallback = iface.address;
            }
        }
    }
    return fallback || 'unbekannt';
}

app.get('/api/status', (req, res) => {
    let clientIp = req.ip || (req.connection && req.connection.remoteAddress) || '';
    if (clientIp.startsWith('::ffff:')) clientIp = clientIp.slice(7);

    res.json({
        ip: getServerIp(),
        clientIp,
        port: PORT,
        lastAccess,
        accessLog,
    });
});

app.get('/api/hardware', (req, res) => {
    res.json(hardwareSnapshot || {});
});

// -------------------------------------------------------------------------
// Server beenden / neu starten
// -------------------------------------------------------------------------

app.post('/api/shutdown', (req, res) => {
    res.json({ message: 'Server wird beendet...' });
    logEvent('Server wird auf Anfrage beendet.');
    publishOffline(() => setTimeout(() => process.exit(0), 300));
});

app.post('/api/restart', (req, res) => {
    res.json({ message: 'Server wird neu gestartet...' });
    logEvent('Server wird auf Anfrage neu gestartet.');
    publishOffline(() => {
        setTimeout(() => {
            const child = spawn(process.execPath, [__filename], {
                detached: true,
                stdio: 'ignore',
                cwd: __dirname,
            });
            child.unref();
            process.exit(0);
        }, 300);
    });
});

function publishOffline(callback) {
    if (mqttClient && mqttClient.connected) {
        mqttClient.publish(AVAILABILITY_TOPIC, 'offline', { qos: 1, retain: true }, () => callback());
    } else {
        callback();
    }
}

// -------------------------------------------------------------------------
// MQTT / Home Assistant Integration
// -------------------------------------------------------------------------

function connectMqtt() {
    mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}:${MQTT_PORT}`, {
        username: MQTT_USERNAME,
        password: MQTT_PASSWORD,
        will: {
            topic: AVAILABILITY_TOPIC,
            payload: 'offline',
            qos: 1,
            retain: true,
        },
        reconnectPeriod: 5000,
    });

    mqttClient.on('connect', () => {
        logEvent(`MQTT verbunden mit ${MQTT_HOST}:${MQTT_PORT}`);
        mqttClient.publish(AVAILABILITY_TOPIC, 'online', { qos: 1, retain: true });
        publishDiscovery();
        publishHardwareDiscovery();
        publishNzbDiscovery();
        publishOverview();
        publishAccessInfo();
        if (hardwareSnapshot) publishHardware();
        publishNzbInfo();
        publishNzbgetStatus();
        publishTotalDownloads();
        // NZBGet-Erreichbarkeit/Warteschlange werden vom nächsten
        // updateNzbgetStatus()-Poll aktuell gehalten (läuft ohnehin alle
        // paar Sekunden).
    });

    mqttClient.on('error', (err) => {
        logEvent('MQTT-Fehler: ' + err.message);
    });
}

function publishDiscoveryEntity(component, objectId, entityConfig) {
    if (!mqttClient) return;
    const fullConfig = Object.assign({
        availability_topic: AVAILABILITY_TOPIC,
        payload_available: 'online',
        payload_not_available: 'offline',
        device: DEVICE_INFO,
    }, entityConfig);

    const topic = `homeassistant/${component}/${DEVICE_ID}/${objectId}/config`;
    mqttClient.publish(topic, JSON.stringify(fullConfig), { qos: 1, retain: true });
}

// Registriert die "Dateianzahl"-Sensoren (Quelle + Ziel) für eine einzelne
// Kategorie bei Home Assistant – wird beim Start für alle Kategorien und
// zusätzlich sofort aufgerufen, wenn über /api/categories eine neue
// Kategorie angelegt wird.
function publishCategoryDiscovery(cat) {
    ['source', 'target'].forEach((section) => {
        const sectionLabel = section === 'source' ? 'Quelle' : 'Ziel';
        const objectId = `${section}_${cat}_count`;
        publishDiscoveryEntity('sensor', objectId, {
            name: `${sectionLabel} ${categoryLabel(cat)} Dateien`,
            unique_id: `${DEVICE_ID}_${objectId}`,
            state_topic: `${MQTT_BASE}/${section}/${cat}/count`,
            json_attributes_topic: `${MQTT_BASE}/${section}/${cat}/attributes`,
            unit_of_measurement: 'Dateien',
            icon: categoryIcon(cat),
        });
    });
}

function publishDiscovery() {
    // Verbindungsstatus als eigener Sensor
    publishDiscoveryEntity('binary_sensor', 'online', {
        name: 'NZB Beamer Online',
        unique_id: `${DEVICE_ID}_online`,
        state_topic: AVAILABILITY_TOPIC,
        payload_on: 'online',
        payload_off: 'offline',
        device_class: 'connectivity',
    });

    // Dateianzahl je Verzeichnis (Quelle + Ziel, für jede Kategorie)
    getCategoryKeys().forEach((cat) => publishCategoryDiscovery(cat));

    // Letzter Zugriff
    publishDiscoveryEntity('sensor', 'last_access', {
        name: 'Letzter Zugriff',
        unique_id: `${DEVICE_ID}_last_access`,
        state_topic: `${MQTT_BASE}/status/last_access`,
        device_class: 'timestamp',
        icon: 'mdi:clock-outline',
    });

    // Zugriffs-IP
    publishDiscoveryEntity('sensor', 'last_client_ip', {
        name: 'Zugriffs-IP',
        unique_id: `${DEVICE_ID}_last_client_ip`,
        state_topic: `${MQTT_BASE}/status/client_ip`,
        icon: 'mdi:ip-network',
    });

    // Letztes Verschieben
    publishDiscoveryEntity('sensor', 'last_move', {
        name: 'Letztes Verschieben',
        unique_id: `${DEVICE_ID}_last_move`,
        state_topic: `${MQTT_BASE}/move/last_run`,
        json_attributes_topic: `${MQTT_BASE}/move/last_result`,
        device_class: 'timestamp',
        icon: 'mdi:file-move',
    });

    // Gesamtzahl aller jemals gebeamten NZBs
    publishDiscoveryEntity('sensor', 'total_downloads', {
        name: 'Gesamt gebeamte NZBs',
        unique_id: `${DEVICE_ID}_total_downloads`,
        state_topic: `${MQTT_BASE}/stats/total_downloads`,
        unit_of_measurement: 'NZBs',
        icon: 'mdi:counter',
        state_class: 'total_increasing',
    });
}

function publishTotalDownloads() {
    if (!mqttClient || !mqttClient.connected) return;
    mqttClient.publish(`${MQTT_BASE}/stats/total_downloads`, String(totalDownloadsCount), { retain: true });
}

function publishOverview() {
    if (!mqttClient || !mqttClient.connected) return;

    getCategoryKeys().forEach((cat) => {
        [['sources', 'source'], ['targets', 'target']].forEach(([key, section]) => {
            const dir = config[key][cat];
            const { files, error } = listDirFiles(dir);
            const totalSize = files.reduce((sum, f) => sum + (f.size || 0), 0);

            mqttClient.publish(`${MQTT_BASE}/${section}/${cat}/count`, String(dir ? files.length : 0), { retain: true });
            mqttClient.publish(`${MQTT_BASE}/${section}/${cat}/attributes`, JSON.stringify({
                path: dir,
                total_size_bytes: totalSize,
                error: error || null,
            }), { retain: true });
        });
    });
}

function publishAccessInfo() {
    if (!mqttClient || !mqttClient.connected) return;
    if (lastAccess) {
        mqttClient.publish(`${MQTT_BASE}/status/last_access`, new Date(lastAccess).toISOString(), { retain: true });
    }
    mqttClient.publish(`${MQTT_BASE}/status/client_ip`, lastClientIp || '', { retain: true });
}

function publishMoveResult(results) {
    if (!mqttClient || !mqttClient.connected) return;
    mqttClient.publish(`${MQTT_BASE}/move/last_run`, new Date().toISOString(), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/move/last_result`, JSON.stringify({ results }), { retain: true });
}

// Übersicht regelmäßig aktualisieren (z. B. falls Dateien außerhalb der
// App im Quellverzeichnis auftauchen)
setInterval(publishOverview, 30000);

// -------------------------------------------------------------------------
// Automatischer Verschiebe-Lauf: prüft die Quellverzeichnisse und verschiebt
// gefundene Ordner (komplett) bzw. lose Dateien ins jeweilige Zielverzeichnis.
// Keine rekursive Datei-Suche nötig, da ganze Ordner als Einheit verschoben
// werden. Kein Dauer-Polling mehr: der Lauf wird gezielt angestoßen, sobald
// der Download einer einzelnen Datei (oder der ganzen Warteschlange) fertig
// ist (siehe updateNzbgetStatus()), plus einmal kurz nach dem Serverstart.
// -------------------------------------------------------------------------

let autoMoveRunning = false;

// Führt einen kompletten Verschiebe-Lauf aus und aktualisiert den geteilten
// Status (lastMoveResults/lastMoveAt, MQTT, Übersicht). Wird sowohl vom
// automatischen Zeitplan als auch vom manuellen Button (/api/move-now)
// verwendet, damit beide denselben Status-Datensatz pflegen.
async function executeMoveAll(trigger) {
    autoMoveRunning = true;
    logEvent(trigger === 'manuell' ? 'Manuelles Verschieben wird ausgeführt…' : 'Automatischer Verzeichnis-Scan wird ausgeführt…');
    try {
        const results = await performMoveAll((step) => {
            if (step.status === 'item-error') {
                logEvent(`  [${step.category}] ${step.message}`);
            }
        });
        publishMoveResult(results);
        publishOverview();
        lastMoveResults = results;
        lastMoveAt = Date.now();
        const summary = results
            .filter((r) => r.status === 'ok' || r.status === 'partial')
            .map((r) => `${categoryLabel(r.category)}: ${r.moved}`)
            .join(', ');
        logEvent(summary ? `Verschieben abgeschlossen – ${summary}` : 'Verschieben abgeschlossen – nichts zu tun');
        return results;
    } catch (err) {
        logEvent((trigger === 'manuell' ? 'Manueller' : 'Automatischer') + ' Verschiebe-Lauf fehlgeschlagen: ' + err.message);
        throw err;
    } finally {
        autoMoveRunning = false;
    }
}

async function runAutoMove() {
    if (autoMoveRunning) return; // vorheriger Lauf noch nicht fertig
    try {
        await executeMoveAll('auto');
    } catch (err) {
        // bereits in executeMoveAll geloggt
    }
}

setTimeout(runAutoMove, 15000); // kurz nach dem Start einmal laufen lassen (z.B. für bereits vorhandene Dateien)

// -------------------------------------------------------------------------
// Hardware-Überwachung: CPU, Speicher, Laufwerke, Netzwerk
// -------------------------------------------------------------------------

function round1(n) {
    return Math.round(n * 10) / 10;
}

function bytesToGb(bytes) {
    return round1(bytes / 1073741824);
}

function sanitizeId(str) {
    return String(str).replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'drive';
}

async function updateHardwareInfo() {
    try {
        const [cpuInfo, cpuLoad, mem, fsSize, netStats, timeInfo] = await Promise.all([
            si.cpu(),
            si.currentLoad(),
            si.mem(),
            si.fsSize(),
            si.networkStats(),
            si.time(),
        ]);

        // Festplatten-Durchsatz separat abfragen: nicht auf allen Systemen
        // verfügbar, darf die übrige Hardware-Erfassung nicht blockieren.
        let diskIO = { readKBs: 0, writeKBs: 0 };
        try {
            const fsStats = await si.fsStats();
            diskIO = {
                readKBs: round1((fsStats.rx_sec || 0) / 1024),
                writeKBs: round1((fsStats.wx_sec || 0) / 1024),
            };
        } catch (e) { /* z.B. unter Windows ohne Berechtigung nicht verfügbar */ }

        const netAgg = netStats.reduce((acc, n) => {
            acc.rx += n.rx_sec || 0;
            acc.tx += n.tx_sec || 0;
            acc.rxBytes += n.rx_bytes || 0;
            acc.txBytes += n.tx_bytes || 0;
            return acc;
        }, { rx: 0, tx: 0, rxBytes: 0, txBytes: 0 });

        hardwareSnapshot = {
            cpu: {
                model: `${cpuInfo.manufacturer} ${cpuInfo.brand}`.trim(),
                cores: cpuInfo.cores,
                speedGhz: cpuInfo.speed,
                loadPercent: round1(cpuLoad.currentLoad),
            },
            memory: {
                totalGb: bytesToGb(mem.total),
                usedGb: bytesToGb(mem.total - mem.available),
                freeGb: bytesToGb(mem.available),
                usedPercent: round1(((mem.total - mem.available) / mem.total) * 100),
            },
            disks: fsSize.map((d) => ({
                mount: d.mount,
                sizeGb: bytesToGb(d.size),
                usedGb: bytesToGb(d.used),
                usedPercent: round1(d.use || 0),
            })),
            diskIO,
            network: {
                rxKBs: round1(netAgg.rx / 1024),
                txKBs: round1(netAgg.tx / 1024),
                totalRxGb: bytesToGb(netAgg.rxBytes),
                totalTxGb: bytesToGb(netAgg.txBytes),
            },
            uptimeSeconds: Math.round(timeInfo.uptime),
            appUptimeSeconds: Math.round(process.uptime()),
            updatedAt: Date.now(),
        };

        publishHardware();
    } catch (err) {
        logEvent('Hardware-Erfassung fehlgeschlagen: ' + err.message);
    }
}

function publishHardwareDiscovery() {
    publishDiscoveryEntity('sensor', 'cpu_load', {
        name: 'CPU-Auslastung',
        unique_id: `${DEVICE_ID}_cpu_load`,
        state_topic: `${MQTT_BASE}/hardware/cpu/load`,
        json_attributes_topic: `${MQTT_BASE}/hardware/cpu/attributes`,
        unit_of_measurement: '%',
        icon: 'mdi:cpu-64-bit',
    });

    publishDiscoveryEntity('sensor', 'memory_usage', {
        name: 'Speicher-Auslastung',
        unique_id: `${DEVICE_ID}_memory_usage`,
        state_topic: `${MQTT_BASE}/hardware/memory/percent`,
        json_attributes_topic: `${MQTT_BASE}/hardware/memory/attributes`,
        unit_of_measurement: '%',
        icon: 'mdi:memory',
    });

    publishDiscoveryEntity('sensor', 'network_down', {
        name: 'Netzwerk Download',
        unique_id: `${DEVICE_ID}_network_down`,
        state_topic: `${MQTT_BASE}/hardware/network/rx`,
        json_attributes_topic: `${MQTT_BASE}/hardware/network/attributes`,
        unit_of_measurement: 'kB/s',
        icon: 'mdi:download-network',
    });

    publishDiscoveryEntity('sensor', 'network_up', {
        name: 'Netzwerk Upload',
        unique_id: `${DEVICE_ID}_network_up`,
        state_topic: `${MQTT_BASE}/hardware/network/tx`,
        json_attributes_topic: `${MQTT_BASE}/hardware/network/attributes`,
        unit_of_measurement: 'kB/s',
        icon: 'mdi:upload-network',
    });

    publishDiscoveryEntity('sensor', 'disk_read', {
        name: 'Festplatte Lesen',
        unique_id: `${DEVICE_ID}_disk_read`,
        state_topic: `${MQTT_BASE}/hardware/disk_io/read`,
        unit_of_measurement: 'kB/s',
        icon: 'mdi:file-download-outline',
    });

    publishDiscoveryEntity('sensor', 'disk_write', {
        name: 'Festplatte Schreiben',
        unique_id: `${DEVICE_ID}_disk_write`,
        state_topic: `${MQTT_BASE}/hardware/disk_io/write`,
        unit_of_measurement: 'kB/s',
        icon: 'mdi:file-upload-outline',
    });

    publishDiscoveryEntity('sensor', 'uptime', {
        name: 'Server-Laufzeit',
        unique_id: `${DEVICE_ID}_uptime`,
        state_topic: `${MQTT_BASE}/hardware/uptime`,
        unit_of_measurement: 's',
        icon: 'mdi:timer-outline',
    });

    publishDiscoveryEntity('sensor', 'app_uptime', {
        name: 'App-Laufzeit',
        unique_id: `${DEVICE_ID}_app_uptime`,
        state_topic: `${MQTT_BASE}/hardware/app_uptime`,
        unit_of_measurement: 's',
        icon: 'mdi:timer-play-outline',
    });
}

function publishNzbDiscovery() {
    publishDiscoveryEntity('sensor', 'current_nzb', {
        name: 'Aktuelle NZB',
        unique_id: `${DEVICE_ID}_current_nzb`,
        state_topic: `${MQTT_BASE}/nzb/name`,
        json_attributes_topic: `${MQTT_BASE}/nzb/attributes`,
        icon: 'mdi:file-download-outline',
    });

    publishDiscoveryEntity('sensor', 'nzbget_progress', {
        name: 'NZBGet-Fortschritt',
        unique_id: `${DEVICE_ID}_nzbget_progress`,
        state_topic: `${MQTT_BASE}/nzb/nzbget_percent`,
        json_attributes_topic: `${MQTT_BASE}/nzb/nzbget_status`,
        unit_of_measurement: '%',
        icon: 'mdi:progress-download',
    });

    publishDiscoveryEntity('binary_sensor', 'nzbget_reachable', {
        name: 'NZBGet erreichbar',
        unique_id: `${DEVICE_ID}_nzbget_reachable`,
        state_topic: `${MQTT_BASE}/nzb/nzbget_reachable`,
        payload_on: 'online',
        payload_off: 'offline',
        device_class: 'connectivity',
    });

    publishDiscoveryEntity('sensor', 'nzbget_queue_count', {
        name: 'NZBGet Warteschlange',
        unique_id: `${DEVICE_ID}_nzbget_queue_count`,
        state_topic: `${MQTT_BASE}/nzb/queue_count`,
        json_attributes_topic: `${MQTT_BASE}/nzb/queue_items`,
        unit_of_measurement: 'Elemente',
        icon: 'mdi:format-list-bulleted',
    });

    // Kurzer Impuls statt Dauerzustand: wechselt für ein paar Sekunden auf
    // "an", sobald die komplette NZBGet-Warteschlange gerade fertig
    // geworden ist, und fällt danach automatisch wieder zurück (off_delay –
    // Home Assistant übernimmt das Zurücksetzen selbst). Eignet sich als
    // Trigger für eine Automation ("Downloads fertig"), bleibt aber nicht
    // dauerhaft "an" hängen.
    publishDiscoveryEntity('binary_sensor', 'all_downloads_done', {
        name: 'NZB-Downloads gerade abgeschlossen',
        unique_id: `${DEVICE_ID}_all_downloads_done`,
        state_topic: `${MQTT_BASE}/nzb/all_done`,
        payload_on: 'done',
        payload_off: 'idle',
        off_delay: 5,
        icon: 'mdi:check-circle-outline',
    });
}

function publishNzbInfo() {
    if (!mqttClient || !mqttClient.connected) return;
    const payload = buildNzbStatusPayload();

    if (!payload) {
        mqttClient.publish(`${MQTT_BASE}/nzb/name`, 'keine', { retain: true });
        mqttClient.publish(`${MQTT_BASE}/nzb/attributes`, JSON.stringify({}), { retain: true });
        return;
    }

    mqttClient.publish(`${MQTT_BASE}/nzb/name`, payload.fileName, { retain: true });
    mqttClient.publish(`${MQTT_BASE}/nzb/attributes`, JSON.stringify({
        total_size_gb: payload.totalSizeGb,
        file_count: payload.fileCount,
        groups: payload.groups,
        received_at: new Date(payload.receivedAt).toISOString(),
    }), { retain: true });
}

function publishNzbgetStatus() {
    if (!mqttClient || !mqttClient.connected) return;
    const payload = nzbgetStatus || { phase: 'none' };
    mqttClient.publish(`${MQTT_BASE}/nzb/nzbget_percent`, String(payload.percent || 0), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/nzb/nzbget_status`, JSON.stringify({
        phase: payload.phase,
        status: payload.status || null,
        downloaded_mb: payload.downloadedMB || 0,
        total_mb: payload.totalMB || 0,
        remaining_mb: payload.remainingMB || 0,
        reachable: nzbgetReachable,
    }), { retain: true });
}

function publishDiskDiscovery(diskId, mountLabel) {
    publishDiscoveryEntity('sensor', `disk_${diskId}`, {
        name: `Laufwerk ${mountLabel} Auslastung`,
        unique_id: `${DEVICE_ID}_disk_${diskId}`,
        state_topic: `${MQTT_BASE}/hardware/disk/${diskId}/percent`,
        json_attributes_topic: `${MQTT_BASE}/hardware/disk/${diskId}/attributes`,
        unit_of_measurement: '%',
        icon: 'mdi:harddisk',
    });
}

function publishHardware() {
    if (!mqttClient || !mqttClient.connected || !hardwareSnapshot) return;

    mqttClient.publish(`${MQTT_BASE}/hardware/cpu/load`, String(hardwareSnapshot.cpu.loadPercent), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/cpu/attributes`, JSON.stringify({
        model: hardwareSnapshot.cpu.model,
        cores: hardwareSnapshot.cpu.cores,
        speed_ghz: hardwareSnapshot.cpu.speedGhz,
    }), { retain: true });

    mqttClient.publish(`${MQTT_BASE}/hardware/memory/percent`, String(hardwareSnapshot.memory.usedPercent), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/memory/attributes`, JSON.stringify({
        total_gb: hardwareSnapshot.memory.totalGb,
        used_gb: hardwareSnapshot.memory.usedGb,
        free_gb: hardwareSnapshot.memory.freeGb,
    }), { retain: true });

    mqttClient.publish(`${MQTT_BASE}/hardware/network/rx`, String(hardwareSnapshot.network.rxKBs), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/network/tx`, String(hardwareSnapshot.network.txKBs), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/network/attributes`, JSON.stringify({
        total_rx_gb: hardwareSnapshot.network.totalRxGb,
        total_tx_gb: hardwareSnapshot.network.totalTxGb,
    }), { retain: true });

    mqttClient.publish(`${MQTT_BASE}/hardware/disk_io/read`, String(hardwareSnapshot.diskIO.readKBs), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/disk_io/write`, String(hardwareSnapshot.diskIO.writeKBs), { retain: true });

    mqttClient.publish(`${MQTT_BASE}/hardware/uptime`, String(hardwareSnapshot.uptimeSeconds), { retain: true });
    mqttClient.publish(`${MQTT_BASE}/hardware/app_uptime`, String(hardwareSnapshot.appUptimeSeconds), { retain: true });

    hardwareSnapshot.disks.forEach((d) => {
        const diskId = sanitizeId(d.mount);
        if (!publishedDiskIds.has(diskId)) {
            publishDiskDiscovery(diskId, d.mount);
            publishedDiskIds.add(diskId);
        }
        mqttClient.publish(`${MQTT_BASE}/hardware/disk/${diskId}/percent`, String(d.usedPercent), { retain: true });
        mqttClient.publish(`${MQTT_BASE}/hardware/disk/${diskId}/attributes`, JSON.stringify({
            mount: d.mount,
            size_gb: d.sizeGb,
            used_gb: d.usedGb,
        }), { retain: true });
    });
}

// Erste Erfassung sofort, danach alle 15 Sekunden
updateHardwareInfo();
setInterval(updateHardwareInfo, 15000);

app.listen(PORT, '0.0.0.0', () => {
    logEvent(`Server gestartet auf Port ${PORT}`);
    logEvent(`Erlaubt: ${(config.networkRanges || []).join(', ')}, localhost`);
    logEvent(`Beam-Ablageordner: ${getUploadDir()}`);
    connectMqtt();
});
