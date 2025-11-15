console.log("=== Script started ===");
console.log("Process ID:", process.pid);
console.log("Node version:", process.version);
console.log("Platform:", process.platform);

// Load environment variables from .env
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const Discord = require('discord.js');
const { joinVoiceChannel, createAudioPlayer, createAudioResource, entersState, VoiceConnectionStatus, AudioPlayerStatus, EndBehaviorType, StreamType } = require('@discordjs/voice');
const { SpeechClient } = require('@google-cloud/speech');
const prism = require('prism-media');
// const { Transform } = require('stream'); // Unused import removed
const mm = require('music-metadata');
// const ytdl = require('ytdl-core'); // Unused import removed
// const ytdlp = require('yt-dlp-exec').raw; // Unused import removed
const ytSearch = require('yt-search');
const { getLyrics } = require('genius-lyrics-api');
const { spawn } = require('child_process');
// const { GoogleSpreadsheet } = require('google-spreadsheet'); // Unused import removed
const { JWT } = require('google-auth-library');
const { google } = require('googleapis');
const ffmpegPath = require('ffmpeg-static');
const { File: MegaFile, Storage: MegaStorage } = (() => { try { return require('megajs'); } catch { return {}; } })();

console.log("🚀 Starting bot initialization...");
console.log("=== Script started ===");
console.log("Process ID:", process.pid);
console.log("Node version:", process.version);
console.log("Platform:", process.platform);

// Ensure Google credentials are available: prefer explicit path, else allow JSON via env for platforms like Koyeb
if (!process.env.GOOGLE_APPLICATION_CREDENTIALS && process.env.GOOGLE_CREDENTIALS_JSON) {
  try {
    const credsPath = '/tmp/google_creds.json';
    fs.writeFileSync(credsPath, process.env.GOOGLE_CREDENTIALS_JSON, { encoding: 'utf8' });
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credsPath;
    console.log('Google credentials written to', credsPath);
  } catch (e) {
    try { console.warn('⚠️ Failed to write GOOGLE_CREDENTIALS_JSON to file:', e?.message || e); } catch {}
  }
}
// Fallback to local creds file only if present (portable)
if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  try {
    const localCreds = path.join(__dirname, 'retard.json');
    if (fs.existsSync(localCreds)) process.env.GOOGLE_APPLICATION_CREDENTIALS = localCreds;
  } catch {}
}

// Fetch helper (node18 global or dynamic import fallback)
const fetch = global.fetch || ((...args) => import('node-fetch').then(({ default: f }) => f(...args)));
async function downloadToFile(url, outPath) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Download failed: ${res.status} ${res.statusText}`);
  const ab = await res.arrayBuffer();
  fs.writeFileSync(outPath, Buffer.from(ab));
}

const speechClient = new SpeechClient();

// Config loading
let fileConfig = {};
try {
  if (fs.existsSync(path.join(__dirname, 'config.json'))) {
    const raw = fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8').trim();
    if (raw) fileConfig = JSON.parse(raw);
  }
} catch {}
const config = {
  MUSIC_FOLDER: fileConfig.MUSIC_FOLDER || './songs',
  SUPPORTED_FORMATS: fileConfig.SUPPORTED_FORMATS || ['.mp3', '.wav', '.ogg'],
  PORT: Number(process.env.PORT || fileConfig.PORT || 3003),
  AUTO_QUEUE_UPLOADS: (process.env.AUTO_QUEUE_UPLOADS != null
    ? String(process.env.AUTO_QUEUE_UPLOADS).toLowerCase() === 'true'
    : (typeof fileConfig.AUTO_QUEUE_UPLOADS === 'boolean' ? fileConfig.AUTO_QUEUE_UPLOADS : false)),
  IMPORT_UPLOADS_TO_LIBRARY: (process.env.IMPORT_UPLOADS_TO_LIBRARY != null
    ? String(process.env.IMPORT_UPLOADS_TO_LIBRARY).toLowerCase() === 'true'
    : (typeof fileConfig.IMPORT_UPLOADS_TO_LIBRARY === 'boolean' ? fileConfig.IMPORT_UPLOADS_TO_LIBRARY : false)),
  // Built-in Juice WRLD metadata/stream provider (player endpoint). Disabled by default.
  JUICEWRLD_PLAYER_ENABLED: (process.env.JUICEWRLD_PLAYER_ENABLED != null
    ? String(process.env.JUICEWRLD_PLAYER_ENABLED).toLowerCase() === 'true'
    : (typeof fileConfig.JUICEWRLD_PLAYER_ENABLED === 'boolean' ? fileConfig.JUICEWRLD_PLAYER_ENABLED : false)),
  JUICEWRLD_API_BASE: process.env.JUICEWRLD_API_BASE || fileConfig.JUICEWRLD_API_BASE || 'https://juicewrldapi.com',
  // Optional endpoint overrides if the site serves HTML to default paths in your region
  JUICEWRLD_SONGS_ENDPOINT: process.env.JUICEWRLD_SONGS_ENDPOINT || fileConfig.JUICEWRLD_SONGS_ENDPOINT || '',
  JUICEWRLD_PLAYER_SONGS_ENDPOINT: process.env.JUICEWRLD_PLAYER_SONGS_ENDPOINT || fileConfig.JUICEWRLD_PLAYER_SONGS_ENDPOINT || '',
  // MEGA streaming
  MEGA_ENABLED: (() => {
    const v = process.env.MEGA_ENABLED ?? fileConfig.MEGA_ENABLED;
    if (v == null) return !!(process.env.MEGA_FOLDER_LINK || fileConfig.MEGA_FOLDER_LINK || process.env.MEGA_EMAIL);
    return String(v).toLowerCase() === 'true';
  })(),
  MEGA_FOLDER_LINK: process.env.MEGA_FOLDER_LINK || fileConfig.MEGA_FOLDER_LINK || '',
  MEGA_FOLDER_KEY: process.env.MEGA_FOLDER_KEY || fileConfig.MEGA_FOLDER_KEY || '',
  MEGA_EMAIL: process.env.MEGA_EMAIL || fileConfig.MEGA_EMAIL || '',
  MEGA_PASSWORD: process.env.MEGA_PASSWORD || fileConfig.MEGA_PASSWORD || '',
  EXTERNAL_STREAM_ENABLED: (process.env.EXTERNAL_STREAM_ENABLED != null
    ? String(process.env.EXTERNAL_STREAM_ENABLED).toLowerCase() === 'true'
    : (typeof fileConfig.EXTERNAL_STREAM_ENABLED === 'boolean' ? fileConfig.EXTERNAL_STREAM_ENABLED : false)),
  EXTERNAL_STREAM_API_URL: process.env.EXTERNAL_STREAM_API_URL || fileConfig.EXTERNAL_STREAM_API_URL || '',
  EXTERNAL_STREAM_API_KEY: process.env.EXTERNAL_STREAM_API_KEY || fileConfig.EXTERNAL_STREAM_API_KEY || ''
};
const DISCORD_TOKEN = process.env.DISCORD_TOKEN || fileConfig.DISCORD_TOKEN || null;
const GENIUS_API_TOKEN = process.env.GENIUS_API_TOKEN || fileConfig.GENIUS_API_TOKEN || '';
// Spotify (bot account) — requires a user refresh token with playlist-modify-public/private and playlist-read-private (optional)
const SPOTIFY_CLIENT_ID = process.env.SPOTIFY_CLIENT_ID || fileConfig.SPOTIFY_CLIENT_ID || '';
const SPOTIFY_CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET || fileConfig.SPOTIFY_CLIENT_SECRET || '';
const SPOTIFY_REFRESH_TOKEN = process.env.SPOTIFY_REFRESH_TOKEN || fileConfig.SPOTIFY_REFRESH_TOKEN || '';
// Optional explicit redirect URI; defaults to http://localhost:PORT/spotify/callback (Spotify allows HTTP only for localhost)
const SPOTIFY_REDIRECT_URI = process.env.SPOTIFY_REDIRECT_URI || fileConfig.SPOTIFY_REDIRECT_URI || `http://localhost:${Number(process.env.PORT || fileConfig.PORT || 3003)}/spotify/callback`;
// Redaction helper to avoid leaking secrets into logs
function redactSecrets(s) {
  try {
    const txt = String(s ?? '');
    const secretHints = [DISCORD_TOKEN, GENIUS_API_TOKEN, process.env.LASTFM_API_KEY, process.env.GOOGLE_PRIVATE_KEY];
    let out = txt;
    for (const val of secretHints) {
      if (!val) continue;
      const snip = String(val);
      if (!snip) continue;
      const safe = snip.length > 8 ? snip.slice(0, 4) + '…' + snip.slice(-4) : '***';
      out = out.split(snip).join(safe);
    }
    return out;
  } catch { return '***'; }
}

console.log("🚀 Starting bot initialization...");

const LASTFM_API_KEY = process.env.LASTFM_API_KEY || fileConfig.LASTFM_API_KEY || '';
// Persisted Last.fm usernames per Discord user
const LASTFM_MAP_FILE = path.join(__dirname, 'lastfm_users.json');
let LASTFM_USER_MAP = {};
try {
  if (fs.existsSync(LASTFM_MAP_FILE)) {
    const raw = fs.readFileSync(LASTFM_MAP_FILE, 'utf8');
    LASTFM_USER_MAP = JSON.parse(raw || '{}');
  }
} catch (e) {
  console.warn('⚠️ Failed to read lastfm_users.json, starting fresh:', e?.message || e);
  LASTFM_USER_MAP = {};
}
function saveLastfmUserMap() {
  try { fs.writeFileSync(LASTFM_MAP_FILE, JSON.stringify(LASTFM_USER_MAP, null, 2)); }
  catch (e) { console.error('❌ Failed to write lastfm_users.json:', e?.message || e); }
}

const client = new Discord.Client({
  intents: [
    Discord.GatewayIntentBits.Guilds,
  Discord.GatewayIntentBits.GuildPresences,
    Discord.GatewayIntentBits.GuildVoiceStates,
    Discord.GatewayIntentBits.GuildMessages,
    Discord.GatewayIntentBits.MessageContent,
    Discord.GatewayIntentBits.GuildMessageReactions
  ],
  partials: [Discord.Partials.Message, Discord.Partials.Channel, Discord.Partials.Reaction]
});

console.log("🤖 Discord client created");

const player = createAudioPlayer();
let connection = null;
const songQueue = [];
let currentSong = null;
let isPlaying = false;
let voiceRecognitionEnabled = true;
let lastVolume = 1.0; // Default volume (100%)
let loweredForSpeech = false;
let autoVolumeEnabled = false; // Auto-volume (ducking) off by default
// Presence-aware playback & empty VC handling
let connectedChannelId = null;
let emptyVcTimeout = null;
let pausedForEmpty = false;
const EMPTY_VC_DISCONNECT_MS = Number(process.env.EMPTY_VC_DISCONNECT_MS || (fileConfig.EMPTY_VC_DISCONNECT_MS ?? 5 * 60 * 1000));

// Autoplay (shuffle local songs) state
let autoPlayEnabled = false;
let autoShuffleDeck = [];
let autoDeckIndex = 0;
// Autoplay source: 'local' (existing) or 'mega'
let autoPlayMode = 'local';
let megaShuffleDeck = [];
let megaDeckIndex = 0;

// MEGA session (optional)
let mega = null; // MegaStorage instance when logged in
let megaFilesIndex = new Map(); // name -> { file?: any, link?: string }

// ===== Wordle-style minigame state (per-channel) =====
const WORDLE_MAX_ATTEMPTS = 6;
const WORDLE_WORDS = [
  'apple','brave','candy','delta','eagle','flame','glory','happy','ivory','jolly',
  'knack','lemon','mango','noble','ocean','pearl','queen','rapid','sunny','tiger',
  'urban','vivid','whale','xenon','young','zesty','angel','bloom','charm','drape',
  'earth','frost','gamer','honey','irony','jazzy','karma','linen','miner','novel',
  'orbit','pixel','quilt','rival','sugar','tempo','umbra','vapor','witty','yield'
];
const wordleGames = new Map(); // channelId -> { target, guesses: string[], finished: boolean, startedAt: number }
function randomWordleWord() { return WORDLE_WORDS[Math.floor(Math.random() * WORDLE_WORDS.length)]; }
function formatWordleFeedback(guess, target) {
  guess = guess.toLowerCase(); target = target.toLowerCase();
  const res = Array(5).fill('⬛');
  const tArr = target.split('');
  const used = Array(5).fill(false);
  // First pass: greens
  for (let i = 0; i < 5; i++) {
    if (guess[i] === target[i]) { res[i] = '🟩'; used[i] = true; }
  }
  // Frequency map for remaining letters in target
  const freq = {};
  for (let i = 0; i < 5; i++) { if (!used[i]) { const ch = tArr[i]; freq[ch] = (freq[ch] || 0) + 1; } }
  // Second pass: yellows
  for (let i = 0; i < 5; i++) {
    if (res[i] === '🟩') continue;
    const ch = guess[i];
    if (freq[ch] > 0) { res[i] = '🟨'; freq[ch]--; }
  }
  return res.join('');
}

// Summarize letter knowledge from guesses vs target
function getWordleLetterSummary(game) {
  try {
    const target = String(game?.target || '').toLowerCase();
    const guesses = Array.isArray(game?.guesses) ? game.guesses : [];
    const tset = new Set(target.split(''));
    const gset = new Set((guesses.join('') || '').toLowerCase().split(''));
    const present = [];
    const absent = [];
    for (const ch of gset) {
      if (!ch || ch < 'a' || ch > 'z') continue;
      if (tset.has(ch)) present.push(ch.toUpperCase()); else absent.push(ch.toUpperCase());
    }
    present.sort();
    absent.sort();
    return { present, absent };
  } catch { return { present: [], absent: [] }; }
}
function renderWordleLetterSummary(game) {
  const { present, absent } = getWordleLetterSummary(game);
  const p = present.length ? present.join(' ') : '—';
  const a = absent.length ? absent.join(' ') : '—';
  return `✅ Present: ${p}\n❌ Absent: ${a}`;
}

// Persistence for Wordle daily limits and recent words (to avoid repeats)
const WORDLE_STATE_FILE = path.join(__dirname, 'wordle_state.json');
const WORDLE_RECENT_WINDOW = 20; // avoid reusing the last N words globally
let WORDLE_STATE = { channelDaily: {}, userDaily: {}, recentWords: [] };
try {
  if (fs.existsSync(WORDLE_STATE_FILE)) {
    const raw = fs.readFileSync(WORDLE_STATE_FILE, 'utf8');
    const loaded = (JSON.parse(raw || '{}') || {});
    WORDLE_STATE = {
      channelDaily: {},
      userDaily: {},
      recentWords: [],
      ...loaded,
      // ensure keys exist if loading older files
      channelDaily: loaded.channelDaily || {},
      userDaily: loaded.userDaily || {},
      recentWords: loaded.recentWords || []
    };
  }
} catch (e) { try { console.warn('⚠️ Failed to load wordle_state.json:', e?.message || e); } catch {} }
function saveWordleState() {
  try { fs.writeFileSync(WORDLE_STATE_FILE, JSON.stringify(WORDLE_STATE, null, 2)); } catch (e) { try { console.warn('⚠️ Failed to save wordle_state.json:', e?.message || e); } catch {} }
}
function todayStr() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`; // local date
}
function pickNonRepeatingWord() {
  const recent = new Set(Array.isArray(WORDLE_STATE.recentWords) ? WORDLE_STATE.recentWords : []);
  const pool = WORDLE_WORDS.filter(w => !recent.has(w));
  let chosen = null;
  if (pool.length > 0) {
    chosen = pool[Math.floor(Math.random() * pool.length)];
  } else {
    // All words recently used; allow the oldest to drop
    chosen = randomWordleWord();
  }
  // Update recent list (FIFO)
  if (!Array.isArray(WORDLE_STATE.recentWords)) WORDLE_STATE.recentWords = [];
  WORDLE_STATE.recentWords.push(chosen);
  if (WORDLE_STATE.recentWords.length > WORDLE_RECENT_WINDOW) WORDLE_STATE.recentWords = WORDLE_STATE.recentWords.slice(-WORDLE_RECENT_WINDOW);
  saveWordleState();
  return chosen;
}

// Allowed guess dictionary (enforced). You can extend by creating wordle_words.json with an array of 5-letter words.
const WORDLE_GUESS_LIST_FILE = path.join(__dirname, 'wordle_words.json');
const WORDLE_ALLOWED_DEFAULT = [
  // Common 5-letter words (compact set)
  'adore','agent','album','alien','anger','antic','apple','april','arena','aside',
  'beach','began','begin','being','belly','bench','berry','blame','blast','bless',
  'blind','block','bloom','board','boast','bonus','boost','brain','brave','bread',
  'break','brick','bride','bring','broad','broke','brown','brush','build','buyer',
  'candy','carry','catch','cause','chain','chair','chalk','charm','chart','chase',
  'cheap','cheer','chest','chief','child','choir','cigar','civic','civil','claim',
  'class','clean','clear','clerk','click','climb','clock','close','cloth','cloud',
  'coach','coast','could','count','court','cover','crack','craft','crash','crazy',
  'cream','crime','cross','crowd','crown','curve','cycle','daily','dance','dealt',
  'death','debut','delay','delta','demon','dense','depth','devil','diary','dirty',
  'doubt','dozen','draft','drain','drama','drawn','dream','dress','drink','drive',
  'eager','eagle','early','earth','eight','elite','email','empty','enjoy','enter',
  'equal','error','event','every','exact','exile','exist','extra','faint','faith',
  'false','fancy','fault','favor','fence','fever','field','fifth','fifty','fight',
  'final','first','flame','flash','fleet','flesh','float','floor','flora','fluid',
  'focus','force','forth','found','fresh','front','fruit','giant','given','glory',
  'grace','grade','grain','grand','grant','grape','graph','grass','great','green',
  'gross','group','guard','guess','guest','guide','habit','happy','heart','heavy',
  'honey','honor','horse','hotel','house','human','humor','ideal','image','index',
  'inner','irony','ivory','jeans','jelly','jerky','joint','jolly','judge','juice',
  'kneel','knife','knock','known','label','labor','large','laser','laugh','layer',
  'learn','lemon','level','light','limit','linen','logic','loyal','lucky','lunch',
  'magic','major','maker','mango','march','metal','meter','micro','minor','mixed',
  'model','money','month','moral','motor','mount','movie','music','naval','nerve',
  'never','noble','noise','north','novel','nurse','ocean','offer','often','onion',
  'orbit','order','other','ought','outer','owner','paint','panel','party','peace',
  'pearl','phase','phone','photo','piano','pilot','place','plant','plate','point',
  'pound','power','press','price','pride','prime','print','prize','proof','proud',
  'queen','quick','quiet','radio','raise','rally','range','rapid','ratio','reach',
  'ready','realm','rebel','refer','relax','renew','reply','rider','ridge','right',
  'rival','river','roast','rough','round','route','royal','rural','sauce','scale',
  'scene','scope','score','screw','seize','sense','serve','seven','sewer','shade',
  'shake','shall','shape','share','sharp','sheep','shelf','shell','shift','shine',
  'shirt','shock','shoot','short','sight','skill','skirt','sleep','slice','slope',
  'small','smart','smell','smile','smoke','snake','solid','solve','sound','south',
  'space','spare','speak','speed','spend','spice','spike','spill','spine','spite',
  'split','spoke','sport','staff','stage','stain','stake','stare','start','state',
  'steam','steel','steep','steer','stick','still','stock','stoke','stone','stood',
  'store','storm','story','strap','straw','strip','stuck','style','sugar','sunny',
  'table','taste','teach','teeth','thank','their','theme','there','thick','thing',
  'think','third','those','three','throw','tiger','tight','title','today','token',
  'touch','tough','tower','trace','track','trade','train','treat','trend','trial',
  'tribe','trick','troop','trust','truth','twice','ultra','under','union','unity',
  'upper','urban','value','vapor','video','visit','vivid','voice','votee','voter',
  'watch','water','whale','wheat','wheel','where','which','white','whole','woman',
  'world','worry','worth','would','write','wrong','yield','young','zesty'
];
let WORDLE_ALLOWED = new Set(WORDLE_ALLOWED_DEFAULT.concat(WORDLE_WORDS));
try {
  if (fs.existsSync(WORDLE_GUESS_LIST_FILE)) {
    const raw = fs.readFileSync(WORDLE_GUESS_LIST_FILE, 'utf8');
    const arr = JSON.parse(raw || '[]');
    if (Array.isArray(arr)) WORDLE_ALLOWED = new Set(arr.map(x => String(x || '').toLowerCase()).filter(w => /^[a-z]{5}$/.test(w)));
  }
} catch (e) { try { console.warn('⚠️ Failed to load wordle_words.json:', e?.message || e); } catch {} }
function isWordleGuessAllowed(word) { return WORDLE_ALLOWED.has(String(word || '').toLowerCase()); }

// Normalize MEGA links and support legacy formats or separate key via env
function normalizeMegaFolderLink(raw) {
  try {
  let s = String(raw || '').trim();
    // strip invisible zero-width characters
  s = s.replace(/[\u200B-\u200D\uFEFF]/g, '');
  // normalize whitespace and dash variants that may appear from copy/paste
  s = s.replace(/\s+/g, '');
  s = s.replace(/[\u2012\u2013\u2014\u2212]/g, '-');
    if (!s) return null;
    // Already contains a hash/key
    if (/#/.test(s)) {
      // Convert legacy format: https://mega.nz/#F!<id>!<key>
      const legacy = s.match(/#F!([^!]+)!([^!#]+)/i);
      if (legacy) {
        const id = legacy[1];
        const key = legacy[2];
        return `https://mega.nz/folder/${id}#${key}`;
      }
      return s;
    }
    // No hash: try to extract id and pair with MEGA_FOLDER_KEY
    const m = s.match(/mega\.nz\/(?:folder)\/([^?#/]+)/i);
    if (m) {
      const id = m[1];
      const key = config.MEGA_FOLDER_KEY;
      if (key) return `https://mega.nz/folder/${id}#${key}`;
      return null; // missing key
    }
    return s; // possibly a file link; leave as-is
  } catch { return null; }
}

function splitMegaFolder(link) {
  try {
    const s = String(link || '').trim();
    const legacy = s.match(/#F!([^!]+)!([^!#]+)/i);
    if (legacy) return { id: legacy[1], key: legacy[2] };
    const modern = s.match(/\/folder\/([^#/?]+)#([^#/?]+)/i);
    if (modern) return { id: modern[1], key: modern[2] };
    const noHash = s.match(/\/folder\/([^#/?]+)/i);
    if (noHash && config.MEGA_FOLDER_KEY) return { id: noHash[1], key: config.MEGA_FOLDER_KEY };
    return null;
  } catch { return null; }
}

function mask(val) {
  try {
    const s = String(val || '');
    if (s.length <= 8) return s ? s[0] + '…' + s.slice(-1) : '';
    return s.slice(0, 4) + '…' + s.slice(-4);
  } catch { return '***'; }
}

function isSupportedAudioName(name) {
  try {
    const ext = path.extname(String(name || '')).toLowerCase();
    return Array.isArray(config.SUPPORTED_FORMATS) && config.SUPPORTED_FORMATS.includes(ext);
  } catch { return false; }
}

async function ensureMegaReadyFromEnv() {
  try {
    if (!config.MEGA_ENABLED || (!MegaStorage && !MegaFile)) return false;
    if (mega && megaFilesIndex.size > 0) return true;
    if (config.MEGA_FOLDER_LINK && MegaFile && MegaFile.fromURL) {
      const normalized = normalizeMegaFolderLink(config.MEGA_FOLDER_LINK);
      if (!normalized) {
        try { console.warn('⚠️ MEGA_FOLDER_LINK is missing its decryption key (#...). Add the key or set MEGA_FOLDER_KEY.'); } catch {}
        return false;
      }
  const parts = splitMegaFolder(normalized);
  try { if (parts) console.log(`🔎 Using MEGA folder id=${mask(parts.id)} key=${mask(parts.key)}`); } catch {}
      megaFilesIndex = new Map();
      const count = await indexMegaFromFolderLink(normalized);
      try { console.log(`☁️ MEGA (public folder) indexed: ${count} files`); } catch {}
      return count > 0;
    } else if (config.MEGA_EMAIL && config.MEGA_PASSWORD && MegaStorage) {
      mega = new MegaStorage({ email: config.MEGA_EMAIL, password: config.MEGA_PASSWORD });
      await new Promise((res, rej) => mega.login((err) => err ? rej(err) : res())).catch(() => null);
    }
    if (!mega) return false;
    megaFilesIndex = new Map();
    const stack = Array.isArray(mega.files) ? [...mega.files] : [];
    while (stack.length) {
      const f = stack.pop();
      if (!f) continue;
      if (f.children && Array.isArray(f.children)) stack.push(...f.children);
      if (f.directory) continue;
      const name = f.name || '';
      if (!isSupportedAudioName(name)) continue;
  const link = f.downloadLink || (typeof f.link === 'function' ? f.link() : '');
  if (name) megaFilesIndex.set((name || '').toLowerCase(), { link: link || '', file: f });
    }
    try { console.log(`☁️ MEGA initialized: ${megaFilesIndex.size} files indexed`); } catch {}
    return megaFilesIndex.size > 0;
  } catch { return false; }
}

function findMegaEntryByName(query) {
  const q = String(query || '').toLowerCase();
  if (!q) return null;
  const exact = megaFilesIndex.get(q);
  if (exact) return exact;
  const hit = Array.from(megaFilesIndex.entries()).find(([name]) => name.includes(q));
  return hit ? hit[1] : null;
}

function createFfmpegResourceFromReadable(readable) {
  const args = [
  '-hide_banner',
  // Probe generously to avoid early EOF
  '-probesize', '50M',
  '-analyzeduration', '100M',
  // Read from stdin
  '-i', 'pipe:0',
  // No video/subs; keep logging at warning for visibility
  '-vn', '-sn', '-loglevel', 'warning',
  // Output 48kHz stereo signed 16-bit little endian PCM
  '-acodec', 'pcm_s16le',
  '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'
  ];
  const proc = spawn(ffmpegPath, args, { stdio: ['pipe', 'pipe', 'pipe'] });
  // Diagnostics: confirm input/output stream activity
  try {
    let gotIn = false;
    let gotOut = false;
    const inTimer = setTimeout(() => { if (!gotIn) try { console.warn('[ffmpeg] No input received within 5s'); } catch {} }, 5000);
    readable.once('data', (chunk) => { gotIn = true; try { console.log(`[ffmpeg] Input started (${chunk?.length || 0} bytes)`); } catch {} });
    proc.stdout.once('data', (chunk) => { gotOut = true; try { console.log(`[ffmpeg] Output started (${chunk?.length || 0} bytes)`); } catch {} });
    proc.stderr.on('data', (d) => { try { const s = d.toString(); if (s.trim()) console.warn('[ffmpeg stderr]', s.trim()); } catch {} });
    proc.on('close', (code, signal) => { try { console.log(`[ffmpeg] exited with code ${code}${signal?`, signal ${signal}`:''}`); } catch {} });
    proc.on('error', (err) => { try { console.error('[ffmpeg] spawn error:', err?.message || err); } catch {} });
    readable.on('error', (err) => { try { console.error('[ffmpeg] input error:', err?.message || err); } catch {} try { proc.stdin.destroy(); } catch {} });
    readable.on('end', () => { try { proc.stdin.end(); } catch {} });
  } catch {}
  // Pipe input into ffmpeg
  readable.on('error', () => { try { proc.stdin.destroy(); } catch {} });
  readable.pipe(proc.stdin);
  return createAudioResource(proc.stdout, { inlineVolume: true, inputType: StreamType.Raw });
}

// Auto-advance and error handlers
player.on(AudioPlayerStatus.Idle, () => {
  if (isPlaying) playNextSong();
});
player.on('error', (err) => {
  console.error('❌ Audio player error:', err);
  playNextSong();
});
player.on(AudioPlayerStatus.Playing, () => {
  try { console.log('🔊 AudioPlayerStatus.Playing'); } catch {}
});

const songInfoInteractions = new Map();
const songsInteractions = new Map();
const songsPageStates = new Map(); // key: messageId -> { artist, songs, page, pageSize }
let sheetSongLibrary = {};
let SONG_META = {};
let SONG_META_BY_BASENAME = {};
// Uzi library and interactions
let uziSongLibrary = {};
const uziInfoInteractions = new Map();

const GOOGLE_SHEETS_CONFIG = {
  SHEET_ID: fileConfig.SHEET_ID || '1sEGQ3fdYwNriE9YDJX2qMXb0SRFJDutEGLQIYQMP2O0'
};
const UZI_GOOGLE_SHEETS_CONFIG = {
  SHEET_ID: process.env.UZI_SHEET_ID || fileConfig.UZI_SHEET_ID || ''
};
// Voice channel rename toggle (off by default)
const VOICE_RENAME_ENABLED = String(process.env.VOICE_RENAME_ENABLED ?? (fileConfig.VOICE_RENAME_ENABLED ?? 'false')).toLowerCase() === 'true';

// Now Playing message state (optional per guild)
const NP_STATE_FILE = path.join(__dirname, 'nowplaying_state.json');
let NP_STATE = {};
try { if (fs.existsSync(NP_STATE_FILE)) NP_STATE = JSON.parse(fs.readFileSync(NP_STATE_FILE, 'utf8') || '{}'); } catch { NP_STATE = {}; }
function saveNpState() { try { fs.writeFileSync(NP_STATE_FILE, JSON.stringify(NP_STATE, null, 2)); } catch {} }

// ================== UFO WEBHOOK (rare daily event) ==================
const UFO_NAME = process.env.UFO_NAME || '🛸 UFO';
const UFO_MESSAGE = process.env.UFO_MESSAGE || '*flies past*';
const UFO_TICK_MINUTES = Number(process.env.UFO_TICK_MINUTES || 30); // how often to roll
const UFO_CHANCE_PER_TICK = Number(process.env.UFO_CHANCE_PER_TICK || 50000); // 1 in N per tick, very low by default
const UFO_STATE_FILE = path.join(__dirname, 'ufo_state.json');
let UFO_STATE = {};
try {
  if (fs.existsSync(UFO_STATE_FILE)) {
    UFO_STATE = JSON.parse(fs.readFileSync(UFO_STATE_FILE, 'utf8') || '{}');
  }
} catch (e) { try { console.warn('⚠️ Failed to read ufo_state.json:', e?.message || e); } catch {} UFO_STATE = {}; }
function saveUfoState() {
  try { fs.writeFileSync(UFO_STATE_FILE, JSON.stringify(UFO_STATE, null, 2)); } catch (e) { console.error('❌ Failed to write ufo_state.json:', e?.message || e); }
}
function todayStr() { return new Date().toISOString().slice(0,10); }

async function pickWebhookChannel(guild, preferredChannelId) {
  try {
    // Prefer system channel if text-based and we can manage webhooks
    const me = guild.members.me || (await guild.members.fetch(client.user.id).catch(() => null));
    const canManage = (ch) => {
      try { return ch && typeof ch.isTextBased === 'function' && ch.isTextBased() && ch.permissionsFor(me)?.has(Discord.PermissionFlagsBits.ManageWebhooks); } catch { return false; }
    };
    const channels = await guild.channels.fetch();
    const eligible = [];
    for (const [,ch] of channels) {
      if (!ch) continue;
      if (ch.type === Discord.ChannelType.GuildText && canManage(ch)) eligible.push(ch);
    }
    // If preferred set and eligible, honor it
    if (preferredChannelId) {
      const chosen = eligible.find(c => c.id === preferredChannelId);
      if (chosen) return chosen;
    }
    // Otherwise system channel if eligible
    const sys = guild.systemChannel;
    if (sys && eligible.find(c => c.id === sys.id)) return sys;
    // Random eligible channel
    if (eligible.length > 0) return eligible[Math.floor(Math.random() * eligible.length)];
    return null;
  } catch { return null; }
}

async function ensureUfoWebhookForGuild(guild) {
  try {
    const gid = guild.id;
    const entry = UFO_STATE[gid] || {};
    const preferred = entry.ufoChannelId || '';
    // If we have a webhook but channel preference changed, recreate in preferred
    if (entry.webhookId && entry.webhookToken && entry.channelId && preferred && entry.channelId !== preferred) {
      try { const wc = new Discord.WebhookClient({ id: entry.webhookId, token: entry.webhookToken }); await wc.delete().catch(() => {}); } catch {}
      UFO_STATE[gid] = { webhookId: '', webhookToken: '', channelId: preferred, lastDate: entry.lastDate || '', ufoChannelId: preferred };
      saveUfoState();
    }
    // If still have a usable webhook, keep it
    if (UFO_STATE[gid]?.webhookId && UFO_STATE[gid]?.webhookToken) {
      return UFO_STATE[gid];
    }
    const ch = await pickWebhookChannel(guild, preferred);
    if (!ch) {
      try { console.log(`🛸 Skipping UFO webhook for ${guild.name}: no suitable channel/permission.`); } catch {}
      return null;
    }
    const wh = await ch.createWebhook({ name: UFO_NAME }).catch(() => null);
    if (!wh) return null;
    UFO_STATE[gid] = { webhookId: wh.id, webhookToken: wh.token, channelId: ch.id, lastDate: UFO_STATE[gid]?.lastDate || '', ufoChannelId: preferred || '' };
    saveUfoState();
    try { console.log(`🛸 Created UFO webhook in #${ch.name} (${guild.name})`); } catch {}
    return UFO_STATE[gid];
  } catch (e) {
    try { console.warn('UFO ensure error:', e?.message || e); } catch {}
    return null;
  }
}

async function trySendUfoForGuild(guild) {
  try {
    const gid = guild.id;
    const entry = await ensureUfoWebhookForGuild(guild);
    if (!entry) return;
    // Only once per day per guild
    if ((entry.lastDate || '') === todayStr()) return;
    // Roll chance
    const roll = Math.floor(Math.random() * UFO_CHANCE_PER_TICK);
    if (roll !== 0) return; // extremely rare
    // Send via webhook
    const whClient = new Discord.WebhookClient({ id: entry.webhookId, token: entry.webhookToken });
    await whClient.send({ content: UFO_MESSAGE, username: UFO_NAME, allowedMentions: { parse: [] } }).catch(async (err) => {
      // If failed (e.g., deleted), recreate once
      try { console.warn('UFO send failed, attempting recreate:', err?.message || err); } catch {}
      const updated = await ensureUfoWebhookForGuild(guild);
      if (!updated) return;
      const wc = new Discord.WebhookClient({ id: updated.webhookId, token: updated.webhookToken });
      await wc.send({ content: UFO_MESSAGE, username: UFO_NAME, allowedMentions: { parse: [] } }).catch(() => {});
    });
    UFO_STATE[gid] = { ...UFO_STATE[gid], lastDate: todayStr() };
    saveUfoState();
  } catch {}
}

function startUfoTicker() {
  try { console.log(`🛸 UFO ticker starting: every ${UFO_TICK_MINUTES}m with 1/${UFO_CHANCE_PER_TICK} chance per guild per tick`); } catch {}
  const run = async () => {
    try {
      const guilds = await client.guilds.fetch();
      for (const [gid] of guilds) {
        const g = await client.guilds.fetch(gid).catch(() => null);
        if (g) await trySendUfoForGuild(g);
      }
    } catch {}
  };
  // Stagger first run a bit to avoid startup spam
  setTimeout(run, Math.min(30000, UFO_TICK_MINUTES * 60 * 1000));
  setInterval(run, Math.max(1, UFO_TICK_MINUTES) * 60 * 1000);
}

async function handleUfoChannelCommand(message, mode, channelText) {
  try {
    if (!message.guild) return void message.reply('❌ Server only.');
    const member = message.member;
    if (!member?.permissions?.has?.(Discord.PermissionFlagsBits.Administrator)) {
      return void message.reply('❌ Admins only.');
    }
    const gid = message.guild.id;
    UFO_STATE[gid] = UFO_STATE[gid] || {};
    const entry = UFO_STATE[gid];
    const act = (mode || '').toLowerCase();
    if (act === 'set') {
      // Resolve channel
      let ch = message.mentions.channels.first() || null;
      if (!ch && channelText) {
        const id = (channelText.match(/\d{15,20}/) || [])[0];
        if (id) ch = await message.guild.channels.fetch(id).catch(() => null);
      }
      if (!ch || ch.type !== Discord.ChannelType.GuildText) return void message.reply('❌ Provide a text channel (mention or ID).');
      // Check bot can manage webhooks there
      const me = message.guild.members.me || (await message.guild.members.fetch(client.user.id).catch(() => null));
      if (!ch.permissionsFor(me)?.has(Discord.PermissionFlagsBits.ManageWebhooks)) return void message.reply('❌ I need Manage Webhooks in that channel.');
      // If existing webhook in a different channel, delete it
      if (entry.webhookId && entry.webhookToken && entry.channelId && entry.channelId !== ch.id) {
        try { const wc = new Discord.WebhookClient({ id: entry.webhookId, token: entry.webhookToken }); await wc.delete().catch(() => {}); } catch {}
        entry.webhookId = ''; entry.webhookToken = '';
      }
      entry.ufoChannelId = ch.id;
      entry.channelId = ch.id;
      saveUfoState();
      // Ensure webhook exists in target channel
      await ensureUfoWebhookForGuild(message.guild);
      return void message.reply(`🛸 UFO channel set to #${ch.name}.`);
    } else if (act === 'random' || act === 'clear' || act === 'unset') {
      // Clear preference and existing webhook so a new random one is created later
      if (entry.webhookId && entry.webhookToken) {
        try { const wc = new Discord.WebhookClient({ id: entry.webhookId, token: entry.webhookToken }); await wc.delete().catch(() => {}); } catch {}
      }
      UFO_STATE[gid] = { lastDate: entry.lastDate || '', webhookId: '', webhookToken: '', channelId: '', ufoChannelId: '' };
      saveUfoState();
      return void message.reply('🛸 UFO channel preference cleared. I will pick a random eligible channel next time.');
    }
    return void message.reply('❌ Usage: !ufochannel set #channel | !ufochannel random');
  } catch (e) {
    console.error('ufochannel error:', e);
    try { await message.reply('❌ Failed to update UFO channel.'); } catch {}
  }
}

keepProcessAlive();

function keepProcessAlive() {
  const http = require('http');
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://localhost:${config.PORT}`);
      if (url.pathname === '/spotify/callback') {
        // Display a minimal confirmation page; token exchange is run manually to obtain refresh token
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        const code = url.searchParams.get('code');
        const err = url.searchParams.get('error');
        res.end(`<html><body>
          <h1>Spotify Auth</h1>
          ${err ? `<p style="color:red;">Error: ${err}</p>` : ''}
          ${code ? `<p>Authorization code received. You can close this tab.</p>` : '<p>No code param found.</p>'}
        </body></html>`);
        return;
      }
      res.writeHead(200);
      res.end(`Discord Bot is Running | Uptime: ${process.uptime().toFixed(0)}s`);
    } catch {
      res.writeHead(200);
      res.end('OK');
    }
  });

  server.listen(config.PORT, () => {
    console.log(`🌐 Keep-alive server running on port ${config.PORT}`);
  });

  const keepAliveInterval = setInterval(() => {
    console.log(`[Keep-alive] Bot still running (${process.uptime().toFixed(0)}s)`);
  }, 60000);

  process.on('SIGINT', () => {
    console.log('🛑 Received SIGINT. Shutting down gracefully...');
    clearInterval(keepAliveInterval);
    try { leaveVoice('SIGINT'); } catch {}
    server.close();
  });

  process.on('SIGTERM', () => {
    console.log('🛑 Received SIGTERM. Shutting down gracefully...');
    clearInterval(keepAliveInterval);
    try { leaveVoice('SIGTERM'); } catch {}
    server.close();
  });
}

// Unified voice cleanup so we always leave the channel on shutdown/restart
function leaveVoice(reason = 'shutdown') {
  try { console.log(`🔻 Leaving voice due to ${reason}...`); } catch {}
  try { if (player) { try { player.stop(true); } catch {} } } catch {}
  try { if (connection) { try { connection.destroy(); } catch {} } } catch {}
  connection = null;
  isPlaying = false;
  currentSong = null;
  songQueue.length = 0;
  connectedChannelId = null;
  pausedForEmpty = false;
  autoPlayEnabled = false;
  autoPlayMode = 'local'; megaShuffleDeck = []; megaDeckIndex = 0; autoShuffleDeck = []; autoDeckIndex = 0;
  clearEmptyDisconnectTimer();
  try { clearVoiceChannelStatus().catch(() => {}); } catch {}
}
// ================== SONG LIBRARY ==================
async function buildSongLibrary() {
  console.log("📀 Building song library...");
  const library = {};
  const meta = {};
  const metaByBase = {};

  try {
    if (!fs.existsSync(config.MUSIC_FOLDER)) {
      fs.mkdirSync(config.MUSIC_FOLDER);
      console.log('📂 Created songs directory');
      return library;
    }

    // Recursively collect files
    async function walk(dir) {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const ent of entries) {
        const full = path.join(dir, ent.name);
        if (ent.isDirectory()) { await walk(full); continue; }
        const ext = path.extname(ent.name).toLowerCase();
        if (!config.SUPPORTED_FORMATS.includes(ext)) continue;
        const rel = path.relative(config.MUSIC_FOLDER, full).split(path.sep).join('/');
        let displayName = path.basename(ent.name, ext).replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).trim();
        let tagArtist = '';
        let tagTitle = '';
        try {
          const metadata = await mm.parseFile(full);
          const title = metadata.common.title;
          const artist = metadata.common.artist;
          tagTitle = title || '';
          tagArtist = artist || '';
          if (title && artist) displayName = `${artist} - ${title}`;
          else if (title) displayName = title;
        } catch {}
        library[rel] = displayName;
        const m = { displayName, artist: tagArtist || 'Unknown Artist', title: tagTitle || displayName, path: full };
        meta[rel] = m;
        const base = path.basename(full);
        if (!metaByBase[base]) metaByBase[base] = m;
      }
    }
    await walk(config.MUSIC_FOLDER);

    console.log(`🎵 Loaded ${Object.keys(library).length} songs`);
    SONG_META = meta;
    SONG_META_BY_BASENAME = metaByBase;
    return library;
  } catch (error) {
    console.error('❌ Error reading music folder:', error);
    return {};
  }
}

let SONG_LIBRARY = {};
buildSongLibrary().then(lib => { SONG_LIBRARY = lib; });

const LAST_CHANNEL_FILE = 'last_channel.json';
const OWNER_ID = fileConfig.OWNER_ID || '465235772870492176';

// ================== SLASH COMMANDS ==================
// Split into guild-only (fast, no DM) and global (DM-capable) to avoid duplicates in servers
const GUILD_SLASH_COMMANDS = [
  { name: 'join', description: 'Join your voice channel' },
  { name: 'play', description: 'Play a song or YouTube URL', options: [{ name: 'query', description: 'Song name or URL', type: 3, required: true }] },
  { name: 'pause', description: 'Pause playback' },
  { name: 'stop', description: 'Stop playback and leave voice channel' },
  { name: 'disconnect', description: 'Disconnect from voice channel' },
  { name: 'queue', description: 'Show the current song queue' },
  { name: 'resume', description: 'Resume playback' },
  { name: 'skip', description: 'Skip the current song' },
  { name: 'volume', description: 'Set playback volume (0-100)', options: [{ name: 'percent', description: 'Volume percent (0-100)', type: 4, required: true, min_value: 0, max_value: 100 }] },
  { name: 'songs', description: 'Browse available songs' },
  { name: 'voiceon', description: 'Enable voice recognition' },
  { name: 'voiceoff', description: 'Disable voice recognition' },
  { name: 'autoplay', description: 'Toggle autoplay to shuffle local songs when the queue is empty', options: [{ name: 'mode', description: 'on or off', type: 3, required: false, choices: [{ name: 'on', value: 'on' }, { name: 'off', value: 'off' }] }] },
  { name: 'snp', description: 'Show Spotify Rich Presence now playing', options: [{ name: 'user', description: 'User to inspect (default: you)', type: 6, required: false }] },
  { name: 'cloneplaylist', description: 'Clone a Spotify playlist to the bot\'s Spotify account', options: [
    { name: 'url', description: 'Spotify playlist URL or ID', type: 3, required: true },
    { name: 'name', description: 'Optional name for the cloned playlist', type: 3, required: false },
    { name: 'privacy', description: 'Public or Private', type: 3, required: false, choices: [{ name: 'public', value: 'public' }, { name: 'private', value: 'private' }] }
  ] },
  { name: 'refresh', description: 'Reload songs library from disk' }
];

const GLOBAL_SLASH_COMMANDS = [
  { name: 'songinfo', description: 'Get detailed Juice WRLD song information', dm_permission: true, integration_types: [1], contexts: [1, 2], options: [{ name: 'query', description: 'Song name', type: 3, required: true }] },
  { name: 'lyrics', description: 'Fetch lyrics for a song', dm_permission: true, integration_types: [1], contexts: [1, 2], options: [{ name: 'query', description: 'Artist - Title or Title', type: 3, required: false }] },
  { name: 'lastfm', description: 'Show your or a user\'s Last.fm now playing', dm_permission: true, integration_types: [0, 1], contexts: [0, 1, 2], options: [{ name: 'username', description: 'Last.fm username (optional if set)', type: 3, required: false }] },
  { name: 'setlastfm', description: 'Link your Last.fm username to your Discord account', dm_permission: true, integration_types: [0, 1], contexts: [0, 1, 2], options: [{ name: 'username', description: 'Your Last.fm username', type: 3, required: true }] },
  { name: 'uziinfo', description: 'Get detailed Lil Uzi Vert song information', dm_permission: true, integration_types: [1], contexts: [1, 2], options: [{ name: 'query', description: 'Song name', type: 3, required: true }] },
  { name: 'nowplaying', description: 'Show the currently playing song', dm_permission: true, integration_types: [0, 1], contexts: [0, 1, 2] },
  { name: 'snp', description: 'Show Spotify Rich Presence now playing', dm_permission: true, integration_types: [1], contexts: [1, 2], options: [{ name: 'user', description: 'User to inspect (default: you)', type: 6, required: false }] },
  { name: 'cloneplaylist', description: 'Clone a Spotify playlist to the bot\'s Spotify account', dm_permission: true, integration_types: [1], contexts: [1, 2], options: [
    { name: 'url', description: 'Spotify playlist URL or ID', type: 3, required: true },
    { name: 'name', description: 'Optional name for the cloned playlist', type: 3, required: false },
    { name: 'privacy', description: 'Public or Private', type: 3, required: false, choices: [{ name: 'public', value: 'public' }, { name: 'private', value: 'private' }] }
  ] }
];

async function registerSlashCommandsForGuild(guildId) {
  try {
    if (!DISCORD_TOKEN) return;
    const rest = new Discord.REST({ version: '10' }).setToken(DISCORD_TOKEN);
  await rest.put(Discord.Routes.applicationGuildCommands(client.user.id, guildId), { body: GUILD_SLASH_COMMANDS });
    try { console.log(`✅ Registered slash commands for guild ${guildId}`); } catch {}
  } catch (e) {
    console.error(`❌ Failed to register slash commands for guild ${guildId}:`, e?.message || e);
  }
}

async function registerGlobalSlashCommands() {
  try {
    if (!DISCORD_TOKEN) return;
    const rest = new Discord.REST({ version: '10' }).setToken(DISCORD_TOKEN);
    await rest.put(Discord.Routes.applicationCommands(client.user.id), { body: GLOBAL_SLASH_COMMANDS });
    try { console.log('✅ Registered global slash commands (DMs)'); } catch {}
  } catch (e) {
  console.error('❌ Failed to register global slash commands:', redactSecrets(e?.message || e));
  }
}

async function clearGlobalSlashCommands() {
  try {
    if (!DISCORD_TOKEN) return;
    const rest = new Discord.REST({ version: '10' }).setToken(DISCORD_TOKEN);
    await rest.put(Discord.Routes.applicationCommands(client.user.id), { body: [] });
    try { console.log('🧹 Cleared all global slash commands'); } catch {}
  } catch (e) {
    console.error('❌ Failed clearing global slash commands:', e?.message || e);
  }
}

async function syncAllSlashCommands() {
  try {
    await clearGlobalSlashCommands();
    await registerGlobalSlashCommands();
    const guilds = await client.guilds.fetch();
    for (const [gid] of guilds) {
      await registerSlashCommandsForGuild(gid);
    }
    return true;
  } catch (e) {
  console.error('❌ Slash sync failed:', redactSecrets(e?.message || e));
    return false;
  }
}

// (Removed duplicate registerGlobalSlashCommands using an undefined SLASH_COMMANDS)

function makeMessageShim(interaction) {
  return {
    author: interaction.user,
  member: interaction.member || null,
    channel: interaction.channel,
    reply: async (content) => {
      const payload = (typeof content === 'string') ? { content } : (content || {});
      // Auto-ephemeral for error-looking messages so they're user-only and dismissable
      try {
        const text = typeof content === 'string' ? content : (content && typeof content.content === 'string' ? content.content : '');
        if (text && (/^❌/.test(text) || /^⚠️/.test(text) || /\berror\b/i.test(text))) {
          payload.ephemeral = true;
        }
      } catch {}
      if (!interaction.deferred && !interaction.replied) return interaction.reply({ ...payload, fetchReply: true });
      return interaction.followUp(payload);
    }
  };
}

// ================== BOT EVENTS ==================
client.on('ready', async () => {
  console.log(`✅ Logged in as ${client.user.tag}!`);
  console.log(`🔗 Server invite: https://discord.com/oauth2/authorize?client_id=${client.user.id}&permissions=3148800&scope=bot%20applications.commands`);
  console.log(`🔗 User install (DM commands): https://discord.com/oauth2/authorize?client_id=${client.user.id}&scope=applications.commands&integration_type=1`);
  console.log(`ℹ️ If you see "Unknown Integration" on the user install link, enable User Install + Command Contexts (DMs) in the Developer Portal, or use the server invite link instead.`);
  client.user.setActivity('!help', { type: Discord.ActivityType.Listening });

  // Load Google Sheets data for song info
  await loadSongInfoFromSheets();
  // Load Uzi sheets if configured
  if (UZI_GOOGLE_SHEETS_CONFIG.SHEET_ID) {
    await loadUziSongInfoFromSheets();
  } else {
    try { console.log('ℹ️ UZI_SHEET_ID not set; /uziinfo will be disabled until configured.'); } catch {}
  }

  // Register slash commands in all guilds
  try {
    const guilds = await client.guilds.fetch();
    for (const [gid] of guilds) await registerSlashCommandsForGuild(gid);
  } catch (e) { console.error('❌ Slash registration failed on ready:', e?.message || e); }
  // Also register globally so they appear in DMs (requires enabling User Install in the Developer Portal)
  await registerGlobalSlashCommands();

  // Start rare UFO webhook ticker
  startUfoTicker();

  // Preload MEGA index from env if configured so you don't have to paste the link
  try {
    if (config.MEGA_ENABLED) {
      const ok = await ensureMegaReadyFromEnv();
      try { console.log(ok ? '☁️ MEGA ready from env.' : 'ℹ️ MEGA not configured or no files indexed.'); } catch {}
    }
  } catch (e) { try { console.warn('⚠️ MEGA preload failed:', e?.message || e); } catch {} }

  // Try to rejoin last channel
  if (fs.existsSync(LAST_CHANNEL_FILE)) {
    try {
      const { guildId, channelId } = JSON.parse(fs.readFileSync(LAST_CHANNEL_FILE, 'utf8'));
      const guild = await client.guilds.fetch(guildId);
      const channel = await guild.channels.fetch(channelId);
      if (channel && channel.isVoiceBased()) {
        // Only auto-rejoin if there is at least one non-bot listener present
        let nonBotCount = 0;
        try { nonBotCount = channel.members ? channel.members.filter(m => !m.user.bot).size : 0; } catch {}
        if (!nonBotCount) {
          try { console.log(`⏭️ Skipping auto-rejoin for ${channel.name}: no non-bot members present.`); } catch {}
          return;
        }
        connection = joinVoiceChannel({
          channelId: channel.id,
          guildId: guild.id,
          adapterCreator: guild.voiceAdapterCreator,
          selfDeaf: false
        });
        await entersState(connection, VoiceConnectionStatus.Ready, 5000);
        connection.subscribe(player);
        console.log(`🔄 Rejoined voice channel: ${channel.name} in ${guild.name}`);
        // Start listening for voice commands after rejoin:
        listenForVoiceCommands(connection, { author: { id: OWNER_ID }, channel });
  connectedChannelId = channel.id; pausedForEmpty = false; clearEmptyDisconnectTimer();
      }
    } catch (err) {
      console.error('❌ Failed to rejoin last voice channel:', err.message || err);
    }
  }
});

client.on('guildCreate', async (guild) => {
  await registerSlashCommandsForGuild(guild.id);
  // Prepare UFO webhook when joining a new guild (best-effort)
  try { await ensureUfoWebhookForGuild(guild); } catch {}
});

client.on('messageCreate', async message => {
  if (message.author.bot) return;
  const rawContent = (typeof message.content === 'string' ? message.content : '') || '';
  const command = rawContent.toLowerCase();
  try {

    if (command === '!join') return void handleJoinCommand(message);
  if (command.startsWith('!play ')) return void handlePlayCommand(message, rawContent.slice(6).trim());
  if (command.startsWith('!jwplay ')) return void handleJwPlayCommand(message, rawContent.slice('!jwplay '.length).trim());
  if (command.startsWith('!jwplayid ')) return void handleJwPlayIdCommand(message, rawContent.slice('!jwplayid '.length).trim());
  if (command.startsWith('!jwsongs')) return void handleJwSongsCommand(message, rawContent.slice('!jwsongs'.length).trim());
    if (command === '!pause') return void handlePauseCommand(message);
    if (command === '!stop') return void handleStopCommand(message);
    if (command === '!disconnect') return void handleDisconnectCommand(message);
    if (command === '!wake') return void handleWakeCommand(message);
    if (command === '!restart') return void handleRestartCommand(message);
    if (command === '!shutdown') return void handleShutdownCommand(message);
  if (command === '!songs list') return void handleListCommand(message);
  if (command === '!songs') return void handleSongsBrowseCommand(message);
    if (command === '!queue') return void handleQueueCommand(message);
    if (command === '!resume') return void handleResumeCommand(message);
    if (command === '!skip') return void handleSkipCommand(message);
  if (command === '!snp') return void handleSpotifyNowPlaying(message, null);
    if (command === '!nowplaying') return void handleNowPlayingCommand(message);
    if (command.startsWith('!volume ')) return void handleVolumeCommand(message, parseInt(command.split(' ')[1]));
  if (command === '!autoplay') return void handleAutoPlayCommand(message, '');
  if (command.startsWith('!autoplay ')) return void handleAutoPlayCommand(message, rawContent.slice('!autoplay '.length));
  if (command === '!megashuffle') return void handleMegaShuffleCommand(message, '');
  if (command.startsWith('!megashuffle ')) return void handleMegaShuffleCommand(message, rawContent.slice('!megashuffle '.length));
    if (command.startsWith('!ufochannel')) {
      const m = rawContent.match(/^!ufochannel\s+(set|random|clear|unset)\s*(.*)$/i);
      const mode = m && m[1] ? m[1] : '';
      const rest = m && m[2] ? m[2] : '';
      return void handleUfoChannelCommand(message, mode, rest);
    }

  if (command === '!refresh') {
      buildSongLibrary().then(lib => {
        SONG_LIBRARY = lib;
        message.reply(`🔄 Reloaded ${Object.keys(SONG_LIBRARY).length} songs`);
  if (autoPlayEnabled) {
    if (autoPlayMode === 'mega') buildMegaShuffleDeck().catch(() => {});
    else buildAutoShuffleDeck().catch(() => {});
  }
      });
      return;
    }

    if (command === '!ping') {
      return void message.reply('🏓 Pong! Bot is online');
    }

    if (command.startsWith('!jwtest')) {
      const m = rawContent.match(/^!jwtest\s*(.*)$/i);
      const q = (m && m[1]) ? m[1].trim() : '';
      return void handleJwTestCommand(message, q);
    }

    if (command.startsWith('!jwsearch ')) {
      const q = rawContent.slice('!jwsearch '.length).trim();
      return void handleJwSearchCommand(message, q);
    }

    if (command === '!spotifyauth') {
      if (!SPOTIFY_CLIENT_ID) return void message.reply('❌ Missing SPOTIFY_CLIENT_ID in .env or config.json');
      const scopes = [
        'playlist-modify-public',
        'playlist-modify-private',
        'playlist-read-private'
      ];
      const params = new URLSearchParams({
        client_id: SPOTIFY_CLIENT_ID,
        response_type: 'code',
        redirect_uri: SPOTIFY_REDIRECT_URI,
        scope: scopes.join(' ')
      });
      const url = `https://accounts.spotify.com/authorize?${params.toString()}`;
      return void message.reply(`Open this to authorize the bot account and obtain a code (copy from the callback):\n${url}\n\nRedirect URI: ${SPOTIFY_REDIRECT_URI}`);
    }

    if (command.startsWith('!cloneplaylist')) {
      // Unified behavior: if no link provided, ask in DM; otherwise clone immediately
      const argText = rawContent.slice('!cloneplaylist'.length).trim();
      const wantsConfidential = argText.toLowerCase() === 'confidential';
      const m = argText.match(/^([^\s]+)(?:\s+\"([^\"]+)\")?(?:\s+(public|private))?$/i);
      const hasUrl = !!(m && m[1]);
      if (!hasUrl || wantsConfidential) {
        try {
          await message.reply('🔒 Please check your DMs to provide the playlist link confidentially.');
          const dm = await message.author.createDM();
          await dm.send('Hi! Reply with the Spotify playlist link you want to clone. Optionally include a name in quotes and privacy (public/private). Example:\nhttps://open.spotify.com/playlist/123... "My Playlist" public');
          const filter = m2 => m2.author.id === message.author.id;
          const collector = dm.createMessageCollector({ filter, max: 1, time: 120000 });
          collector.on('collect', async m2 => {
            const mm = m2.content.trim().match(/^([^\s]+)(?:\s+\"([^\"]+)\")?(?:\s+(public|private))?$/i);
            const urlOrId = mm && mm[1] ? mm[1] : '';
            const name = mm && mm[2] ? mm[2] : '';
            const privacy = mm && mm[3] ? mm[3] : '' 
            if (!urlOrId) return void dm.send('❌ No playlist link detected. Please try again.');
            await handleClonePlaylistCommand({
              reply: async (msg) => { try { await message.channel.send(msg); } catch {} },
              author: message.author,
              channel: message.channel
            }, { urlOrId, name, privacy });
            await dm.send('✅ Clone requested. I posted the result in your channel.');
          });
          collector.on('end', (collected) => {
            if (collected.size === 0) dm.send('❌ No playlist link received. Clone cancelled.');
          });
        } catch {
          await message.reply('❌ Could not DM you. Please enable DMs from server members.');
        }
        return;
      }
      const urlOrId = m[1];
      const name = m[2] || '';
      const privacy = m[3] || '';
      await handleClonePlaylistCommand(message, { urlOrId, name, privacy });
      return;
    }

  if (command === '!slashsync') {
      if (message.author.id !== OWNER_ID) return void message.reply('❌ Not authorized.');
      const ok = await syncAllSlashCommands();
      return void message.reply(ok ? '✅ Slash commands synced (global + all guilds). It may take a few minutes to propagate.' : '❌ Failed to sync slash commands. Check logs.');
    }

    if (command === '!userinstall') {
  const userLink = `https://discord.com/oauth2/authorize?client_id=${client.user.id}&scope=applications.commands&integration_type=1`;
  const guildLink = `https://discord.com/oauth2/authorize?client_id=${client.user.id}&permissions=3148800&scope=bot%20applications.commands`;
  return void message.reply(`Install options:
• User install (DM commands): ${userLink}
• Server install (bot + commands): ${guildLink}

If the user install shows "Unknown Integration", enable User Install + Command Contexts (DMs) for this app in the Developer Portal, or use the server install link above.`);
    }

    if (command === '!slashdump') {
      if (message.author.id !== OWNER_ID) return void message.reply('❌ Not authorized.');
      try {
        const rest = new Discord.REST({ version: '10' }).setToken(DISCORD_TOKEN);
        const globals = await rest.get(Discord.Routes.applicationCommands(client.user.id));
        const summary = Array.isArray(globals) ? globals.map(c => ({ name: c.name, id: c.id, dm_permission: c.dm_permission, contexts: c.contexts, integration_types: c.integration_types })).slice(0, 50) : globals;
        const text = 'Global commands registered (first 50):\n' + JSON.stringify(summary, null, 2);
        if (text.length < 1800) return void message.reply('```json\n' + text + '\n```');
        const tmp = path.join(__dirname, 'slashdump.json');
        fs.writeFileSync(tmp, JSON.stringify(summary, null, 2));
        try { await message.reply({ content: '📄 Global commands dump attached.', files: [tmp] }); }
        finally { try { fs.unlinkSync(tmp); } catch {} }
      } catch (e) {
        return void message.reply('❌ Failed to dump global commands: ' + (e?.message || e));
      }
    }

  if (command === '!help') {
      return void message.reply({
        embeds: [{
          title: "Bot Commands",
          description: [
            "`!join` - Join your voice channel",
            "`!play [song]` - Play a song (prefers MEGA and JW API when enabled)",
            "`!pause` - Pause playback",
            "`!stop` - Stop playback and leave",
            "`!disconnect` - Disconnect from voice channel",
            "`!songs` - List available songs",
            "`!refresh` - Reload songs library",
            "`!ping` - Check if bot is online",
            "`!wake` - Start listening for voice commands",
            "`!restart` - Restart the bot",
            "`!shutdown` - Shut down the bot",
            "`!queue` - Show the current song queue",
            "`!resume` - Resume playback",
            "`!skip` - Skip the current song",
            "`!nowplaying` - Show the currently playing song",
            "`!volume [0-100]` - Set playback volume",
            "`!autovolumeon` | `!autovolumeoff` - Toggle auto-volume during speech",
            "`!autoplay on|off` - Shuffle local songs when the queue is empty",
            "`!megashuffle on|off` - Shuffle MEGA files when the queue is empty (MEGA must be configured)",
            "`!snp` - Show your Spotify Rich Presence (or use /snp [user])",
            "`!wordle start` - Start a Wordle-style game in this channel",
            "`!guess <5-letter>` - Guess a word for the current game",
            "`!wordle status` - Show guesses so far",
            "`!wordle giveup` - Reveal the word and end the game",
            "`!wordle letters` - Show which letters are present/absent so far",
            "`!megalist` - List MEGA files (uses MEGA_FOLDER_LINK if set)",
            "`!megalogin [email:password | folderLink]` - Optional: override or initialize MEGA; omit args to use MEGA_FOLDER_LINK",
            "`!songinfo [name]` - Get detailed Juice WRLD song information",
            "`!jwplay [name]` - Queue a JW track by name (uses API index)",
            "`!jwplayid [id]` - Queue a JW track by numeric id",
            "`!jwsongs <query> [page=N] [size=M] [cat=...]` - Search JW /songs",
            "`!rcplay [name]` - Play from a remote catalog (set REMOTE_CATALOG_URL)",
            "`!rclist [query]` - List top matches from the remote catalog",
            "`!lastfm <username>` - Show a user's Last.fm now playing",
            "`!cloneplaylist <urlOrId> \"Name\" public|private` - Clone a Spotify playlist to the bot account",
          ].join('\n'),
          color: 0x00FF00
        }]
      });
    }

    // Attachment upload (silent)
  if (message.attachments.size > 0) {
      for (const attachment of message.attachments.values()) {
        const ext = path.extname(attachment.name).toLowerCase();
        if (config.SUPPORTED_FORMATS.includes(ext)) {
          const filePath = path.join(config.MUSIC_FOLDER, attachment.name);
          await downloadToFile(attachment.url, filePath);
          if (config.IMPORT_UPLOADS_TO_LIBRARY) {
            SONG_LIBRARY = await buildSongLibrary();
            if (config.AUTO_QUEUE_UPLOADS && connection) {
              const resource = createAudioResource(filePath, { inlineVolume: true });
              songQueue.push({ resource, title: SONG_LIBRARY[attachment.name] || attachment.name, source: 'upload' });
              if (!isPlaying) playNextSong();
            }
          }
        } else {
          // silent on unsupported types too
        }
      }
      return;
    }

    // Wordle-style minigame
    if (command === '!wordle start') {
      const chId = message.channel?.id;
      if (!chId) return;
      const userId = message.author?.id;
      const existing = wordleGames.get(chId);
      if (existing && !existing.finished) return void message.reply('❌ A game is already running. Use `!wordle status` or `!wordle giveup`.');
      // Enforce 2 games per channel per day
      const today = todayStr();
      const userKey = `${userId}:${today}`;
      const uCount = WORDLE_STATE.userDaily[userKey]?.count || 0;
      if (uCount >= 2) return void message.reply('⏱️ Daily limit reached for you (2 games). Try again tomorrow.');
      const target = pickNonRepeatingWord();
      wordleGames.set(chId, { target, guesses: [], finished: false, startedAt: Date.now() });
      // Increment daily count and persist
      WORDLE_STATE.userDaily[userKey] = { count: uCount + 1 };
      saveWordleState();
      return void message.reply('🧩 Wordle started! Use `!guess <5-letter>` to play. You have 6 attempts.');
    }
    if (command.startsWith('!guess ')) {
      const chId = message.channel?.id; if (!chId) return;
      const game = wordleGames.get(chId);
      if (!game || game.finished) return void message.reply('❌ No active game. Start one with `!wordle start`.');
      const guess = command.slice('!guess '.length).trim().toLowerCase();
  if (!/^[a-z]{5}$/.test(guess)) return void message.reply('❌ Guess must be a 5-letter word (A–Z).');
  if (!isWordleGuessAllowed(guess)) return void message.reply('❌ Not a recognized word. Try another 5-letter word.');
      if (game.guesses.includes(guess)) return void message.reply('⚠️ You already tried that word.');
      const feedback = formatWordleFeedback(guess, game.target);
      game.guesses.push(guess);
      if (guess === game.target) {
        game.finished = true;
        const letters = renderWordleLetterSummary(game);
        return void message.reply(`✅ ${feedback}  — Correct! You solved it in ${game.guesses.length}/${WORDLE_MAX_ATTEMPTS}.\n${letters}`);
      }
      if (game.guesses.length >= WORDLE_MAX_ATTEMPTS) {
        game.finished = true;
        const letters = renderWordleLetterSummary(game);
        return void message.reply(`❌ ${feedback}  — Out of attempts! The word was: **${game.target}**.\n${letters}`);
      }
      const letters = renderWordleLetterSummary(game);
      return void message.reply(`${feedback}  — Attempts: ${game.guesses.length}/${WORDLE_MAX_ATTEMPTS}\n${letters}`);
    }
    if (command === '!wordle status') {
      const chId = message.channel?.id; if (!chId) return;
      const game = wordleGames.get(chId);
      if (!game) return void message.reply('ℹ️ No game in this channel. Use `!wordle start`.');
      const lines = game.guesses.map(g => `${g.toUpperCase()}  ${formatWordleFeedback(g, game.target)}`);
      const state = game.finished ? `Finished — word was ${game.target.toUpperCase()}` : `In progress — Attempts: ${game.guesses.length}/${WORDLE_MAX_ATTEMPTS}`;
      const letters = renderWordleLetterSummary(game);
      const body = lines.length ? (lines.join('\n') + `\n${state}`) : state;
      return void message.reply(`${body}\n${letters}`);
    }
    if (command === '!wordle giveup') {
      const chId = message.channel?.id; if (!chId) return;
      const game = wordleGames.get(chId);
      if (!game) return void message.reply('ℹ️ No game to give up.');
      game.finished = true;
      return void message.reply(`🏳️ The word was **${game.target.toUpperCase()}**.`);
    }
    if (command === '!wordle letters') {
      const chId = message.channel?.id; if (!chId) return;
      const game = wordleGames.get(chId);
      if (!game) return void message.reply('ℹ️ No game in this channel. Use `!wordle start` to begin.');
      return void message.reply(renderWordleLetterSummary(game));
    }

    if (command === '!voiceon') {
      voiceRecognitionEnabled = true;
      return void message.reply('🎤 Voice recognition enabled!');
    }
    if (command === '!voiceoff') {
      voiceRecognitionEnabled = false;
      return void message.reply('🔇 Voice recognition disabled!');
    }

    if (command.startsWith('!lyrics')) {
      return void handleLyricsCommand(message, rawContent.slice(7).trim());
    }

    if (/^!uziinfo\b/i.test(rawContent)) {
      const m = rawContent.match(/^!uziinfo\b\s*(.*)$/i);
      const rawQuery = (m && m[1]) ? m[1].trim() : '';
      return void handleUziInfoQuery(message, rawQuery);
    }

    if (command === '!autovolumeon') {
      autoVolumeEnabled = true;
      return void message.reply('🔊 Auto-volume enabled!');
    }
    if (command === '!autovolumeoff') {
      autoVolumeEnabled = false;
      return void message.reply('🔇 Auto-volume disabled!');
    }

    if (command === '!songcount') {
      return void message.reply(`📊 Sheet entries loaded: ${Object.keys(sheetSongLibrary).length}`);
    }

    if (command === '!reloadsheets') {
      await loadSongInfoFromSheets();
      return void message.reply(`🔄 Reloaded Google Sheets. Entries: ${Object.keys(sheetSongLibrary).length}`);
    }

    if (command.startsWith('!coverdebug')) {
      const m = rawContent.match(/^!coverdebug\s*(.*)$/i);
      const q = (m && m[1]) ? m[1].trim() : '';
      return void handleCoverDebugCommand(message, q);
    }

    if (/^!songinfo\b/i.test(rawContent)) {
      const m = rawContent.match(/^!songinfo\b\s*(.*)$/i);
      const rawQuery = (m && m[1]) ? m[1].trim() : '';
      return void handleSongInfoQuery(message, rawQuery);
    }
    
    // Link Last.fm username
    if (command.startsWith('!setlastfm')) {
      const m = rawContent.match(/^!setlastfm\s+(.+)$/i);
      const user = m && m[1] ? m[1].trim() : '';
      if (!user) return void message.reply('❌ Usage: !setlastfm <username>');
      LASTFM_USER_MAP[message.author.id] = user;
      saveLastfmUserMap();
      return void message.reply(`✅ Linked Last.fm username to ${user}. Use !lastfm with no args next time.`);
    }

    // Last.fm now playing/last played
    if (command.startsWith('!lastfm')) {
      const m = rawContent.match(/^!lastfm(?:\s+(.+))?$/i);
      let user = m && m[1] ? m[1].trim() : '';
      if (!user) user = LASTFM_USER_MAP[message.author.id] || '';
      if (!user) return void message.reply('❌ No Last.fm username set. Use !setlastfm <username> or pass one to !lastfm <username>.');
      return void handleLastfmNowPlaying(message, user);
    }
    // MEGA: play a public file link directly, or by filename if logged into a folder
    if (command.startsWith('!megaplay ')) return void handleMegaPlayCommand(message, rawContent.slice('!megaplay '.length).trim());
    if (command === '!megalogin' || command.startsWith('!megalogin ')) return void handleMegaLoginCommand(message, rawContent.slice('!megalogin '.length).trim());
    if (command === '!megalist') return void handleMegaListCommand(message);
  if (command === '!megainfo') return void handleMegaInfoCommand(message);

    if (command.startsWith('!rcplay ')) return void handleRcPlayCommand(message, rawContent.slice('!rcplay '.length).trim());
    if (command.startsWith('!rclist ')) return void handleRcListCommand(message, rawContent.slice('!rclist '.length).trim());
    // Diagnostics
    if (command === '!audiotest') return void handleAudioTestCommand(message);
    if (command === '!vcdiag') return void handleVcDiagCommand(message);

  } catch (error) {
    console.error('❌ Command error:', error);
    try { await message.reply(`❌ Error: ${error.message}`); } catch {}
  }
});

client.on('interactionCreate', async (interaction) => {
  try {
    // Slash commands (Chat Input)
    if (interaction.isChatInputCommand && interaction.isChatInputCommand()) {
      const name = interaction.commandName;
      const msg = makeMessageShim(interaction);
      const isDM = interaction.guildId == null;
      try {
        switch (name) {
          case 'join':
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            await interaction.deferReply();
            return void handleJoinCommand(msg);
          case 'play': {
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            const q = interaction.options.getString('query', true);
            await interaction.deferReply();
            return void handlePlayCommand(msg, q);
          }
          case 'pause':
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            await interaction.deferReply();
            return void handlePauseCommand(msg);
          case 'stop':
          case 'disconnect':
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            await interaction.deferReply();
            return void handleStopCommand(msg);
          case 'queue':
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            await interaction.deferReply();
            return void handleQueueCommand(msg);
          case 'resume':
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            await interaction.deferReply();
            return void handleResumeCommand(msg);
          case 'skip':
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            await interaction.deferReply();
            return void handleSkipCommand(msg);
          case 'snp': {
            const userOpt = interaction.options.getUser('user') || null;
            // Avoid deferring so errors can be returned as an initial ephemeral reply
            return void handleSpotifyNowPlaying(msg, userOpt);
          }
          case 'nowplaying':
            await interaction.deferReply();
            return void handleNowPlayingCommand(msg);
          case 'volume': {
            if (isDM) return void interaction.reply({ content: '❌ This command requires a server voice channel.', ephemeral: true });
            const v = interaction.options.getInteger('percent', true);
            await interaction.deferReply();
            return void handleVolumeCommand(msg, v);
          }
          case 'songs':
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            await interaction.deferReply();
            return void handleSongsBrowseCommand(msg);
          case 'autoplay': {
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            const mode = interaction.options.getString('mode') || '';
            await interaction.deferReply();
            return void handleAutoPlayCommand(msg, mode);
          }
          case 'songinfo': {
            const q = interaction.options.getString('query', true);
            await interaction.deferReply();
            return void handleSongInfoQuery(msg, q);
          }
          case 'lyrics': {
            const q = interaction.options.getString('query') || '';
            await interaction.deferReply();
            return void handleLyricsCommand(msg, q);
          }
          case 'cloneplaylist': {
            const urlOrId = interaction.options.getString('url', true);
            const name = interaction.options.getString('name') || '';
            const privacy = interaction.options.getString('privacy') || '';
            await interaction.deferReply();
            return void handleClonePlaylistCommand(msg, { urlOrId, name, privacy });
          }
          case 'uziinfo': {
            const q = interaction.options.getString('query', true);
            await interaction.deferReply();
            return void handleUziInfoQuery(msg, q);
          }
          case 'lastfm': {
            let user = interaction.options.getString('username');
            if (!user) user = LASTFM_USER_MAP[interaction.user.id] || '';
            if (!user) return void interaction.reply({ content: '❌ No Last.fm username set. Use /setlastfm or pass a username.', ephemeral: true });
            await interaction.deferReply();
            return void handleLastfmNowPlaying(msg, user);
          }
          case 'setlastfm': {
            const user = interaction.options.getString('username', true);
            LASTFM_USER_MAP[interaction.user.id] = user;
            saveLastfmUserMap();
            return void interaction.reply({ content: `✅ Linked your Last.fm username to ${user}.`, ephemeral: true });
          }
          case 'voiceon':
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            await interaction.deferReply();
            voiceRecognitionEnabled = true;
            return void msg.reply('🎤 Voice recognition enabled!');
          case 'voiceoff':
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            await interaction.deferReply();
            voiceRecognitionEnabled = false;
            return void msg.reply('🔇 Voice recognition disabled!');
          case 'refresh':
            if (isDM) return void interaction.reply({ content: '❌ This command works in servers only.', ephemeral: true });
            await interaction.deferReply();
            buildSongLibrary().then(lib => { SONG_LIBRARY = lib; msg.reply(`🔄 Reloaded ${Object.keys(SONG_LIBRARY).length} songs`); if (autoPlayEnabled) { if (autoPlayMode === 'mega') buildMegaShuffleDeck().catch(() => {}); else buildAutoShuffleDeck().catch(() => {}); } });
            return;
        }
      } catch (e) {
        console.error('Slash handler error:', e);
        try { if (!interaction.replied) await interaction.reply({ content: '❌ Error running command.', ephemeral: true }); } catch {}
      }
      return;
    }

    // Component interactions
    if (!interaction.isStringSelectMenu()) return;
    // song info selection
    if (interaction.customId.startsWith('songinfo_select:')) {
      const token = interaction.customId.split(':')[1];
      const list = songInfoInteractions.get(token);
      if (!list) {
        return void interaction.reply({ content: '⚠️ This selection has expired. Please run the command again.', ephemeral: true });
      }
      const idx = parseInt(interaction.values[0], 10);
      const song = list[idx];
      if (!song) {
        return void interaction.reply({ content: '❌ Invalid selection.', ephemeral: true });
      }
      const payload = buildEmbedPayload(song);
      await interaction.update({ components: [] });
      try { await interaction.message.edit(payload); }
      catch (err1) {
        console.error('Edit with attachment failed, trying followUp:', err1?.message || err1);
        try { await interaction.followUp({ ...payload, ephemeral: false }); try { await interaction.message.delete(); } catch {} }
        catch (err2) {
          console.error('followUp with attachment failed, falling back to embed only:', err2?.message || err2);
          const embedOnly = { embeds: [buildSongEmbed(song)] };
          try { await interaction.followUp({ ...embedOnly, ephemeral: false }); try { await interaction.message.delete(); } catch {} }
          catch (err3) { console.error('Final fallback failed:', err3?.message || err3); }
        }
      }
      songInfoInteractions.delete(token);
      return;
    }

    // songs browsing: artist first
    if (interaction.customId.startsWith('songs_artist:')) {
      const token = interaction.customId.split(':')[1];
      const state = songsInteractions.get(token);
      if (!state) return void interaction.reply({ content: '⚠️ This selection expired. Run !songs again.', ephemeral: true });
      const artistKey = interaction.values[0];
      const songs = (state.byArtist.get(artistKey) || []).slice().sort((a,b) => a.displayName.localeCompare(b.displayName));
      const { embed, page, totalPages } = buildArtistSongsEmbed(artistKey, songs, 1, 25);
      await interaction.update({ embeds: [embed], components: [] });
      // Setup reaction-based pagination if multiple pages
      try {
        const msg = await interaction.message.fetch();
        if (totalPages > 1) {
          // Prime reactions
          await msg.react('◀️');
          await msg.react('▶️');
          // Track state
          songsPageStates.set(msg.id, { artist: artistKey, songs, page, pageSize: 25, userId: interaction.user.id });
          const filter = (reaction, user) => {
            if (user.bot) return false;
            // limit to the user who made the selection for less spam
            const st = songsPageStates.get(msg.id);
            const allowed = !st?.userId || user.id === st.userId;
            return allowed && (reaction.emoji.name === '◀️' || reaction.emoji.name === '▶️');
          };
          const collector = msg.createReactionCollector({ filter, time: 5 * 60 * 1000 });
          collector.on('collect', async (reaction, user) => {
            try {
              const st = songsPageStates.get(msg.id);
              if (!st) return;
              if (reaction.emoji.name === '▶️') st.page = (st.page % Math.max(1, Math.ceil(st.songs.length / st.pageSize))) + 1;
              if (reaction.emoji.name === '◀️') st.page = (st.page - 2 + Math.max(1, Math.ceil(st.songs.length / st.pageSize))) % Math.max(1, Math.ceil(st.songs.length / st.pageSize)) + 1;
              const next = buildArtistSongsEmbed(st.artist, st.songs, st.page, st.pageSize);
              await msg.edit({ embeds: [next.embed] });
              // Try to remove user's reaction to allow repeated paging; ignore permission errors
              try { await reaction.users.remove(user.id); } catch {}
              songsPageStates.set(msg.id, { ...st, page: next.page });
            } catch {}
          });
          collector.on('end', async () => {
            try { await msg.reactions.removeAll(); } catch {}
            songsPageStates.delete(msg.id);
          });
        } else {
          songsPageStates.delete(msg.id);
        }
      } catch {}
      return;
    }

    if (interaction.customId.startsWith('songs_song:')) {
      const token = interaction.customId.split(':')[1];
      const state = songsInteractions.get(token);
      if (!state) return void interaction.reply({ content: '⚠️ This selection expired. Run !songs again.', ephemeral: true });
      const file = interaction.values[0];
      const entry = state.all.find(e => e.file === file);
      if (!entry) return void interaction.reply({ content: '❌ Invalid selection.', ephemeral: true });
      // Do not queue; instruct how to play
      await interaction.reply({ content: `To play: !play ${entry.file}`, ephemeral: true });
      return;
    }

    // uzi info selection
    if (interaction.customId.startsWith('uziinfo_select:')) {
      const token = interaction.customId.split(':')[1];
      const list = uziInfoInteractions.get(token);
      if (!list) return void interaction.reply({ content: '⚠️ This selection has expired. Please run the command again.', ephemeral: true });
      const idx = parseInt(interaction.values[0], 10);
      const song = list[idx];
      if (!song) return void interaction.reply({ content: '❌ Invalid selection.', ephemeral: true });
      const payload = buildUziEmbed(song);
      await interaction.update({ components: [] });
      try { await interaction.message.edit(payload); }
      catch (err1) { try { await interaction.followUp({ ...payload, ephemeral: false }); try { await interaction.message.delete(); } catch {} } catch {} }
      uziInfoInteractions.delete(token);
      return;
    }
  } catch (err) {
  console.error('❌ Interaction error:', redactSecrets(err?.message || err));
    try { if (!interaction.replied && !interaction.deferred) await interaction.reply({ content: '❌ Something went wrong handling your selection.', ephemeral: true }); } catch {}
  }
});

// ================== SPOTIFY PLAYLIST CLONE ==================
async function getSpotifyAccessToken() {
  if (!SPOTIFY_CLIENT_ID || !SPOTIFY_CLIENT_SECRET || !SPOTIFY_REFRESH_TOKEN) {
    throw new Error('Missing Spotify credentials. Set SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN.');
  }
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: SPOTIFY_REFRESH_TOKEN
  });
  const b64 = Buffer.from(`${SPOTIFY_CLIENT_ID}:${SPOTIFY_CLIENT_SECRET}`).toString('base64');
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: { 'Authorization': `Basic ${b64}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error('Spotify token refresh failed: ' + redactSecrets(`${res.status} ${res.statusText} ${txt}`));
  }
  const j = await res.json();
  return j.access_token;
}

function parseSpotifyPlaylistId(urlOrId) {
  const s = String(urlOrId || '').trim();
  if (!s) return '';
  // Accept raw ID or https://open.spotify.com/playlist/{id} or spotify:playlist:{id}
  const m1 = s.match(/open\.spotify\.com\/playlist\/([a-zA-Z0-9]+)(?:\?|$)/i);
  if (m1) return m1[1];
  const m2 = s.match(/spotify:playlist:([a-zA-Z0-9]+)/i);
  if (m2) return m2[1];
  return s; // assume it's an ID
}

async function spotifyApiFetch(pathname, { method = 'GET', token, body, query } = {}) {
  const url = new URL(`https://api.spotify.com/v1${pathname}`);
  if (query) for (const [k, v] of Object.entries(query)) url.searchParams.set(k, String(v));
  const res = await fetch(url, {
    method,
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`Spotify API ${method} ${url.pathname} failed: ${res.status} ${res.statusText} ${txt}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

async function handleClonePlaylistCommand(message, { urlOrId, name, privacy }) {
  try {
    const access = await getSpotifyAccessToken();
    const pid = parseSpotifyPlaylistId(urlOrId);
    if (!pid) return void message.reply('❌ Invalid playlist URL or ID.');

    // Get source playlist details
    const src = await spotifyApiFetch(`/playlists/${pid}`, { token: access });
    const sourceName = src?.name || 'Cloned Playlist';
    const cloneName = name && name.trim() ? name.trim() : `${sourceName} (clone)`;
    const isPublic = String(privacy || '').toLowerCase() !== 'private';

    // Who am I?
    const me = await spotifyApiFetch('/me', { token: access });
    const userId = me?.id;
    if (!userId) return void message.reply('❌ Failed to resolve bot Spotify user id.');

    // Create new playlist
    const created = await spotifyApiFetch(`/users/${encodeURIComponent(userId)}/playlists`, {
      token: access,
      method: 'POST',
      body: { name: cloneName, public: isPublic, description: `Cloned by Discord bot on ${new Date().toISOString()}` }
    });
    const newPid = created?.id;
    const newUrl = created?.external_urls?.spotify;
    if (!newPid) return void message.reply('❌ Failed creating destination playlist.');

    // Paginate items from source and copy
    let nextUrl = src?.tracks?.href || `/playlists/${pid}/tracks`;
    let totalAdded = 0;
    let localSkipped = 0;
    while (nextUrl) {
      const pageUrl = nextUrl.startsWith('http') ? nextUrl : `https://api.spotify.com/v1${nextUrl}`;
      const page = await fetch(pageUrl, { headers: { Authorization: `Bearer ${access}` } });
      if (!page.ok) return void message.reply('❌ Spotify page fetch failed: ' + page.status);
      const pj = await page.json();
      const items = Array.isArray(pj.items) ? pj.items : [];
      // Collect URIs, skip local tracks
      const uris = [];
      for (const it of items) {
        const tr = it.track;
        if (!tr || tr.is_local) { localSkipped++; continue; }
        if (tr.uri) uris.push(tr.uri);
      }
      // Add in batches of 100
      for (let i = 0; i < uris.length; i += 100) {
        const batch = uris.slice(i, i + 100);
        if (batch.length === 0) continue;
        await spotifyApiFetch(`/playlists/${newPid}/tracks`, { token: access, method: 'POST', body: { uris: batch } });
        totalAdded += batch.length;
      }
      nextUrl = pj.next || null;
    }

    const lines = [
      `✅ Cloned playlist to: ${newUrl || `https://open.spotify.com/playlist/${newPid}`}`,
      `Tracks added: ${totalAdded}`,
      ...(localSkipped ? [`Local/unsupported skipped: ${localSkipped}`] : [])
    ];
    return void message.reply(lines.join('\n'));
  } catch (e) {
    console.error('cloneplaylist error:', e);
    return void message.reply('❌ Failed to clone playlist: ' + (e?.message || e));
  }
}

async function playNextSong() {
  if (songQueue.length === 0) {
    // If empty and autoplay enabled, enqueue one from deck
    if (autoPlayEnabled) {
  const injected = autoPlayMode === 'mega' ? await enqueueNextMegaAutoTrack() : await enqueueNextAutoTrack();
      if (!injected) {
        isPlaying = false; currentSong = null; try { await clearVoiceChannelStatus(); } catch {} ; return;
      }
    } else {
      isPlaying = false; currentSong = null; try { await clearVoiceChannelStatus(); } catch {} ; return;
    }
  }
  // If no listeners, defer playback and schedule disconnect
  if (!hasNonBotListeners()) {
    pausedForEmpty = true;
    scheduleEmptyDisconnect();
    return;
  }
  isPlaying = true;
  const next = songQueue.shift();
  let resource;
  try {
    if (next.resource) {
      resource = next.resource;
    } else if (next.type === 'youtube') {
      const ytdlpPath = fs.existsSync(path.join(__dirname, 'yt-dlp.exe')) ? path.join(__dirname, 'yt-dlp.exe') : 'yt-dlp';
      const streamProc = spawn(ytdlpPath, ['-f', 'bestaudio', '-o', '-', next.url], { stdio: ['ignore', 'pipe', 'ignore'] });
      resource = createAudioResource(streamProc.stdout, { inlineVolume: true });
    } else if (next.type === 'external') {
      // Use ffmpeg to read/normalize and pipe into an opus-encodable stream
      if (!ffmpegPath) throw new Error('ffmpeg not available');
      const args = [
        '-reconnect', '1',
        '-reconnect_streamed', '1',
        '-reconnect_delay_max', '5',
      ];
      // Add headers for JW API sources to satisfy potential referer/UA checks
      if (next.source === 'juicewrld') {
        const headerLines = [
          'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
          `Referer: ${config.JUICEWRLD_API_BASE || 'https://juicewrldapi.com'}`,
          'Accept: */*'
        ].join('\r\n');
        args.push('-headers', headerLines);
      }
      args.push(
        '-i', next.url,
        '-analyzeduration', '0',
        '-loglevel', 'error',
        '-f', 's16le',
        '-ar', '48000',
        '-ac', '2',
        'pipe:1'
      );
      const proc = spawn(ffmpegPath, args, { stdio: ['ignore', 'pipe', 'ignore'] });
      resource = createAudioResource(proc.stdout, { inlineVolume: true, inputType: undefined });
    } else if (next.type === 'file') {
      resource = createAudioResource(next.path, { inlineVolume: true });
    } else {
      throw new Error('Unknown song type');
    }
  currentSong = next; pausedForEmpty = false; clearEmptyDisconnectTimer();
  // Safety: ensure volume > 0 in case ducking stuck
  try { if (resource?.volume && typeof resource.volume.setVolume === 'function') { if ((resource.volume.volume ?? 1) <= 0) resource.volume.setVolume(lastVolume || 1.0); } } catch {}
    player.play(resource);
    const src = next.source || next.type || 'unknown';
    console.log(`🎶 Now playing: ${next.title} [${src}]`);
    try {
      const srcShort = src === 'juicewrld' ? 'JW API' : src === 'external' ? 'API' : src === 'direct' ? 'URL' : src === 'local' ? 'Local' : src === 'youtube' ? 'YT' : '';
      const labelCore = next.auto ? `Now playing (Autoplay): ${next.title}` : `Now playing: ${next.title}`;
      const label = srcShort ? `${labelCore} [${srcShort}]` : labelCore;
      setVoiceChannelStatus(label).catch(() => {});
    } catch {}
  } catch (error) {
    console.error('❌ Playback failed:', error);
    currentSong = null;
    playNextSong();
  }
}

async function handleStopCommand(message) {
  if (connection) {
    player.stop(true);
    connection.destroy();
    connection = null;
    songQueue.length = 0;
    isPlaying = false;
  autoPlayEnabled = false;
  autoPlayMode = 'local'; megaShuffleDeck = []; megaDeckIndex = 0;
  currentSong = null; connectedChannelId = null; pausedForEmpty = false; clearEmptyDisconnectTimer();
  try { clearVoiceChannelStatus().catch(() => {}); } catch {}
    message.reply('🛑 Stopped playback and left voice channel.');
  } else {
    message.reply('❌ Bot is not in voice channel!');
  }
}

async function handleJoinCommand(message) {
  try {
    const guild = message.guild;
    const member = message.member;
    if (!guild || !member) return void message.reply('❌ This command works in servers only.');
    const voice = member.voice?.channel;
    if (!voice || !voice.isVoiceBased?.()) return void message.reply('❌ You must be in a voice channel.');
    connection = joinVoiceChannel({ channelId: voice.id, guildId: guild.id, adapterCreator: guild.voiceAdapterCreator, selfDeaf: false });
    await entersState(connection, VoiceConnectionStatus.Ready, 10_000);
    connection.subscribe(player);
    connectedChannelId = voice.id; pausedForEmpty = false; clearEmptyDisconnectTimer();
    fs.writeFileSync(LAST_CHANNEL_FILE, JSON.stringify({ guildId: guild.id, channelId: voice.id }));
    listenForVoiceCommands(connection, message);
    return void message.reply(`✅ Joined ${voice.name}.`);
  } catch (e) {
    console.error('join error:', e);
    return void message.reply('❌ Failed to join your channel.');
  }
}

async function handlePlayCommand(message, query) {
  try {
    const text = (query || '').trim();
    if (!text) return void message.reply('❌ Usage: !play <local filename | search | YouTube URL>');
    // Ensure connected
    if (!connection) {
      await handleJoinCommand(message);
      if (!connection) return; // join failed
    }

    // New: Prefer MEGA by default (except explicit URLs handled first)
    let queued = false;

    // 1) Explicit URL
    if (isUrl(text)) {
      // Special-case MEGA URLs to stream via megajs directly
      if (/^https?:\/\/mega\.nz\//i.test(text) && config.MEGA_ENABLED && MegaFile) {
        const norm = normalizeMegaFolderLink(text) || text;
        const file = MegaFile.fromURL(norm);
        let readable = null;
        try { if (typeof file.download === 'function') readable = file.download({ start: 0 }); } catch {}
        if (!readable) { try { if (typeof file.createReadStream === 'function') readable = file.createReadStream({ initialChunkSize: 1024*1024, chunkSize: 1024*1024 }); } catch {} }
        if (readable) {
          const resource = createFfmpegResourceFromReadable(readable);
          songQueue.push({ resource, title: file.name || 'MEGA Audio', source: 'external' });
          queued = true;
        } else {
          songQueue.push({ type: 'external', url: text, title: 'External Stream', source: 'direct' });
          queued = true;
        }
      } else if (isYouTubeUrl(text)) {
        songQueue.push({ type: 'youtube', url: text, title: 'YouTube Audio', source: 'youtube' });
        queued = true;
      } else {
        songQueue.push({ type: 'external', url: text, title: 'External Stream', source: 'direct' });
        queued = true;
      }
    }

    // 2) MEGA by name (default path)
    if (!queued && config.MEGA_ENABLED) {
      await ensureMegaReadyFromEnv();
      if (megaFilesIndex.size > 0) {
        const entry = findMegaEntryByName(text);
        if (entry && (entry.file || (entry.link && MegaFile))) {
          const file = entry.file || (entry.link && MegaFile.fromURL(entry.link));
          if (file) {
            let readable = null;
            try { if (typeof file.download === 'function') readable = file.download({ start: 0 }); } catch {}
            if (!readable) { try { if (typeof file.createReadStream === 'function') readable = file.createReadStream({ initialChunkSize: 1024*1024, chunkSize: 1024*1024 }); } catch {} }
            if (readable) {
              const resource = createFfmpegResourceFromReadable(readable);
              songQueue.push({ resource, title: file.name || text, source: 'external' });
              queued = true;
            } else if (entry.link) {
              songQueue.push({ type: 'external', url: entry.link, title: text, source: 'external' });
              queued = true;
            }
          }
        }
      }
    }

    // 3) Juice WRLD provider (if enabled)
    if (!queued && config.JUICEWRLD_PLAYER_ENABLED) {
      const jw = await resolveJuiceWrldPlayer(text);
      if (jw.ok) {
        songQueue.push({ type: 'external', url: jw.url, title: jw.title || text, source: 'juicewrld' });
        queued = true;
      }
    }

    // 4) Direct local file name match
    if (!queued) {
      let libFile = SONG_LIBRARY[text] ? text : null;
      if (!libFile) {
        const candidates = Object.keys(SONG_LIBRARY).filter(k => path.basename(k).toLowerCase() === text.toLowerCase());
        if (candidates.length === 1) libFile = candidates[0];
        else if (candidates.length > 1) libFile = candidates.sort((a,b) => a.length - b.length)[0];
      }
      if (libFile) {
        const filePath = path.join(config.MUSIC_FOLDER, libFile);
        const title = SONG_LIBRARY[libFile] || libFile;
        songQueue.push({ type: 'file', path: filePath, title, source: 'local' });
        queued = true;
      }
    }

    // 5) Generic external provider hook
    if (!queued && config.EXTERNAL_STREAM_ENABLED) {
      const hintArtist = 'Juice WRLD';
      const ext = await resolveExternalStream({ query: text, artistHint: hintArtist });
      if (ext.ok) {
        const title = ext.title || text;
        songQueue.push({ type: 'external', url: ext.url, title, source: 'external' });
        queued = true;
      }
    }

    // 6) Local fuzzy match
    if (!queued) {
      const { bestMatch } = findBestMatchingSong(text);
      if (bestMatch) {
        const filePath = path.join(config.MUSIC_FOLDER, bestMatch);
        const title = SONG_LIBRARY[bestMatch] || bestMatch;
        songQueue.push({ type: 'file', path: filePath, title, source: 'local' });
        queued = true;
      }
    }

    // 7) Fallback: YouTube search
    if (!queued) {
      const r = await ytSearch(text + ' Juice WRLD audio');
      const v = r && r.videos && r.videos.length > 0 ? r.videos[0] : null;
      if (v && v.url) {
        songQueue.push({ type: 'youtube', url: v.url, title: v.title || text, source: 'youtube' });
        queued = true;
      }
    }

    if (!queued) return void message.reply('❌ Could not resolve that track.');
    if (!isPlaying) playNextSong();
    return void message.reply('✅ Added to queue.');
  } catch (e) {
    console.error('play error:', e);
    return void message.reply('❌ Failed to queue that.');
  }
}

async function handlePauseCommand(message) {
  if (!connection) return message.reply('❌ Bot not in voice channel!');
  if (player.state.status !== AudioPlayerStatus.Playing) return message.reply('⏸️ Nothing is currently playing.');
  player.pause();
  message.reply('⏸️ Paused playback.');
  try { setVoiceChannelStatus('Paused').catch(() => {}); } catch {}
}

async function handleResumeCommand(message) {
  if (!connection) return message.reply('❌ Bot not in voice channel!');
  if (player.state.status !== AudioPlayerStatus.Paused) return message.reply('▶️ Playback is not paused.');
  if (!hasNonBotListeners()) {
    pausedForEmpty = true; scheduleEmptyDisconnect();
    return message.reply('⚠️ No listeners in the voice channel. I will resume when someone joins.');
  }
  clearEmptyDisconnectTimer(); pausedForEmpty = false;
  player.unpause();
  message.reply('▶️ Resumed playback.');
  try { if (currentSong?.title) setVoiceChannelStatus(`Now playing: ${currentSong.title}`).catch(() => {}); } catch {}
}

async function handleListCommand(message) {
  if (Object.keys(SONG_LIBRARY).length === 0) return message.reply('❌ No songs found! Add files to the /songs folder.');
  const songLines = Object.entries(SONG_LIBRARY).map(([file, name]) => `• ${name} (\`${file}\`)`);
  const MAX_CHUNK = 4096;
  const chunks = [];
  let chunk = '';
  for (const line of songLines) {
    if ((chunk + line + '\n').length > MAX_CHUNK) { chunks.push(chunk); chunk = ''; }
    chunk += line + '\n';
  }
  if (chunk) chunks.push(chunk);
  const totalPages = chunks.length;
  for (let i = 0; i < chunks.length; i++) {
    await message.reply({ embeds: [{ title: `🎵 Available Songs (${Object.keys(SONG_LIBRARY).length})${totalPages > 1 ? ` [Page ${i + 1}/${totalPages}]` : ''}`, description: chunks[i], color: 0x7289DA }] });
  }
}

function findBestMatchingSong(searchQuery) {
  if (!searchQuery) return { bestMatch: null, score: 0 };
  const q = searchQuery.toLowerCase();
  const searchTerms = q.split(/\s+/);
  let bestMatch = null;
  let highestScore = 0;
  for (const [file, displayName] of Object.entries(SONG_LIBRARY)) {
    let score = 0;
    const fileLower = file.toLowerCase();
    const baseLower = path.basename(file).toLowerCase();
    const nameLower = displayName.toLowerCase();
    for (const term of searchTerms) {
      if (fileLower.includes(term)) score += 1;
      if (baseLower.includes(term)) score += 2;
      if (nameLower.includes(term)) score += 3;
    }
    if (nameLower === q || baseLower === q) score += 5; // exact match boost
    if (score > highestScore) { highestScore = score; bestMatch = file; }
  }
  return { bestMatch, score: highestScore };
}

async function handleDisconnectCommand(message) {
  if (connection) {
    connection.destroy();
    connection = null;
  autoPlayEnabled = false;
  autoPlayMode = 'local'; megaShuffleDeck = []; megaDeckIndex = 0; autoShuffleDeck = []; autoDeckIndex = 0;
  connectedChannelId = null; pausedForEmpty = false; clearEmptyDisconnectTimer();
  try { clearVoiceChannelStatus().catch(() => {}); } catch {}
  message.reply('👋 Disconnected from the voice channel.');
  } else {
    message.reply('❌ Bot is not in a voice channel!');
  }
}

async function handleWakeCommand(message) {
  if (!connection) return message.reply('❌ Bot is not in a voice channel! Use !join first.');
  listenForVoiceCommands(connection, message);
  message.reply('👂 Listening for voice commands now!');
}

async function handleRestartCommand(message) {
  if (message.author.id !== OWNER_ID) return message.reply('❌ You are not authorized to restart the bot.');
  await message.reply('🔄 Restarting bot...');
  try { leaveVoice('restart command'); } catch {}
  process.exit(2);
}

async function handleShutdownCommand(message) {
  if (message.author.id !== OWNER_ID) return message.reply('❌ You are not authorized to shut down the bot.');
  await message.reply('🛑 Shutting down bot...');
  try { leaveVoice('shutdown command'); } catch {}
  process.exit(0);
}

async function handleSpotifyNowPlaying(message, targetUser) {
  try {
    // Resolve presence source: in guild use that guild; in DMs search mutual guilds
    let member = null;
    if (message.guild) {
      member = targetUser ? await message.guild.members.fetch(targetUser.id).catch(() => null) : message.member;
    } else {
      const user = targetUser || message.author;
      const guilds = await client.guilds.fetch().catch(() => null);
      if (guilds) {
        for (const [gid] of guilds) {
          const g = await client.guilds.fetch(gid).catch(() => null);
          if (!g) continue;
          const m = await g.members.fetch(user.id).catch(() => null);
          if (m) { member = m; break; }
        }
      }
    }
    if (!member) return void message.reply('❌ Could not resolve that user in any mutual server.');
    // Check presences intent availability indirectly
    const activities = member.presence?.activities || [];
    const spotify = activities.find(a => a.type === Discord.ActivityType.Listening && a.name === 'Spotify');
    if (!spotify) return void message.reply('❌ No Spotify activity found for that user.');
    // Discord’s Spotify activity exposes details
    const title = spotify.details || 'Unknown Title';
    const artist = spotify.state || 'Unknown Artist'; // typically "Artist1; Artist2"
    const album = spotify.assets?.largeText || '';
    const largeImage = spotify.assets?.largeImage || '';
    const started = spotify.timestamps?.start || 0;
    const ends = spotify.timestamps?.end || 0;
    const totalMs = (ends && started && ends > started) ? (ends - started) : 0;
    const posMs = started ? (Date.now() - started) : 0;
    const embed = new Discord.EmbedBuilder()
      .setTitle(title)
      .setColor(0x1DB954);
    const fields = [];
    if (album) fields.push({ name: 'Album', value: String(album).slice(0,1024), inline: true });
    if (artist) fields.push({ name: 'Artist', value: String(artist).slice(0,1024), inline: true });
    fields.push({ name: 'Time', value: `${formatTimeMs(posMs)} / ${totalMs ? formatTimeMs(totalMs) : '—:—'}`, inline: true });
    embed.addFields(fields);
    // Try to build cover image from Spotify asset key
    // largeImage is like 'spotify:ab67616d0000b273xxxxxxxxxxxxxxxxxxxx'
    if (largeImage && largeImage.startsWith('spotify:')) {
      const key = largeImage.split(':')[1];
      const url = `https://i.scdn.co/image/${key}`;
      embed.setThumbnail(url);
    }
    return void message.reply({ embeds: [embed] });
  } catch (e) {
    console.error('snp error:', e);
    return void message.reply('❌ Failed to read Spotify presence (make sure the Privileged Intent for Guild Presences is enabled).');
  }
}

async function handleQueueCommand(message) {
  if (songQueue.length === 0 && !isPlaying) return message.reply('🎶 The queue is currently empty.');
  let queueMsg = '';
  if (isPlaying && currentSong) queueMsg += `▶️ **Now playing:** ${currentSong.title || 'Unknown'}\n`;
  if (songQueue.length > 0) queueMsg += songQueue.map((item, i) => `${i + 1}. ${item.title || 'Unknown'}`).join('\n');
  if (!queueMsg) queueMsg = '🎶 The queue is currently empty.';
  message.reply({ embeds: [{ title: '🎵 Song Queue', description: queueMsg, color: 0x7289DA }] });
}

async function handleSkipCommand(message) {
  if (!connection || !isPlaying) return message.reply('❌ Nothing is playing!');
  player.stop();
  message.reply('⏭️ Skipped to the next song.');
}

// Guess artist and title for the current track using SONG_META or display name
function guessArtistAndTitleForCurrent() {
  try {
    let artist = '';
    let title = currentSong?.title || '';
    if (currentSong && currentSong.type === 'file' && currentSong.path) {
      const fname = path.basename(currentSong.path);
      const meta = SONG_META[fname];
      if (meta) {
        if (meta.artist && meta.artist !== 'Unknown Artist') artist = meta.artist;
        if (meta.title) title = meta.title;
      }
    }
    if ((!artist || !title) && currentSong?.title && currentSong.title.includes(' - ')) {
      const parts = currentSong.title.split(' - ');
      if (!artist && parts[0]) artist = parts[0].trim();
      if (!title && parts[1]) title = parts.slice(1).join(' - ').trim();
    }
    return { artist, title };
  } catch { return { artist: '', title: currentSong?.title || '' }; }
}

// Helper to build the minimal Now Playing embed + optional cover file
async function buildNowPlayingMinimalPayload() {
  try {
    if (!isPlaying || !currentSong) return { content: '❌ Nothing is playing!' };
    let title = currentSong.title || '';
    let album = '';
    let artist = '';
    let picture = null;
    let totalMs = 0;
    let timeFieldValue = '';
    // Extract tags if local file
    if (currentSong.type === 'file' && currentSong.path && fs.existsSync(currentSong.path)) {
      try {
        const meta = await mm.parseFile(currentSong.path);
        if (meta?.common?.title) title = meta.common.title;
        if (meta?.common?.album) album = meta.common.album;
        if (meta?.common?.artist) artist = meta.common.artist;
        const pics = meta?.common?.picture;
        if (Array.isArray(pics) && pics.length > 0) picture = pics[0];
        if (meta?.format?.duration) totalMs = Math.round(meta.format.duration * 1000);
      } catch {}
    }
    // Fallback album bracket parsing
    if (!album && title.includes('[') && title.includes(']')) {
      const m = title.match(/\[(.+?)\]/);
      if (m) album = m[1];
    }
    const embed = new Discord.EmbedBuilder()
      .setTitle(title || (currentSong.title || 'Unknown Title'))
      .setColor(0x1DB954);
    const src = currentSong?.source || currentSong?.type || '';
    const srcLabel = src === 'juicewrld' ? 'Juice WRLD API' : src === 'external' ? 'External API' : src === 'direct' ? 'Direct URL' : src === 'local' ? 'Local Library' : src === 'youtube' ? 'YouTube' : '';
  if (album) embed.addFields({ name: 'Album', value: album.slice(0, 1024), inline: true });
    if (artist) embed.addFields({ name: 'Artist', value: artist.slice(0, 1024), inline: true });
    try {
      const posMs = (player?.state?.resource?.playbackDuration) || 0;
      timeFieldValue = `${formatTimeMs(posMs)} / ${totalMs > 0 ? formatTimeMs(totalMs) : '—:—'}`;
      embed.addFields({ name: 'Time', value: timeFieldValue, inline: true });
    } catch {}
  if (srcLabel) embed.addFields({ name: 'Source', value: srcLabel, inline: true });
    // Attach cover from file when available
    if (picture && picture.data && picture.format) {
      const buf = Buffer.from(picture.data);
      const ext = picture.format.toLowerCase().includes('png') ? 'png' : 'jpg';
      const name = `cover.${ext}`;
      embed.setThumbnail(`attachment://${name}`);
      return { embeds: [embed], files: [{ attachment: buf, name }] };
    }
    // Fallback: sheet cover finder best-effort
    const best = findSheetSongByName(title || currentSong.title || '');
    const payload = best ? buildEmbedPayload(best) : null;
    if (payload && payload.files && payload.files.length > 0) {
      const minimal = new Discord.EmbedBuilder().setTitle(title || currentSong.title || 'Unknown Title').setColor(0x1DB954);
      if (album) minimal.addFields({ name: 'Album', value: album.slice(0, 1024), inline: true });
      if (artist) minimal.addFields({ name: 'Artist', value: artist.slice(0, 1024), inline: true });
      if (timeFieldValue) minimal.addFields({ name: 'Time', value: timeFieldValue, inline: true });
      const file = payload.files[0];
      minimal.setThumbnail(minimal.data?.thumbnail?.url || `attachment://${file.name || 'cover.jpg'}`);
      return { embeds: [minimal], files: [file] };
    }
    return { embeds: [embed] };
  } catch (e) {
    try { console.error('buildNowPlayingMinimalPayload error:', e?.message || e); } catch {}
    return { content: `🎶 Now playing: ${currentSong?.title || 'Unknown'}` };
  }
}

async function handleVolumeCommand(message, vol) {
  if (!connection) return message.reply('❌ Bot not in voice channel!');
  if (isNaN(vol) || vol < 0 || vol > 100) return message.reply('❌ Volume must be 0-100.');
  const res = player.state.resource;
  if (res && res.volume) {
    res.volume.setVolume(vol / 100);
    lastVolume = vol / 100;
    message.reply(`🔊 Volume set to ${vol}%`);
  } else {
    message.reply('❌ No audio resource to set volume.');
  }
}

async function handleNowPlayingCommand(message) {
  if (!isPlaying || !currentSong) return message.reply('❌ Nothing is playing!');
  try {
    const replyPayload = await buildNowPlayingMinimalPayload();
    const replyMsg = await message.reply(replyPayload);
    // Add reactions: ⏮️ (restart), ⏭️ (skip), ℹ️ (details)
    try {
      await replyMsg.react('⏮️');
      await replyMsg.react('⏭️');
      await replyMsg.react('ℹ️');
      const allowedUserId = message.author?.id;
      const filter = (reaction, user) => {
        if (user.bot) return false;
        if (allowedUserId && user.id !== allowedUserId) return false;
        const name = reaction.emoji.name;
        return name === '⏮️' || name === '⏭️' || name === 'ℹ️';
      };
      const collector = replyMsg.createReactionCollector({ filter, time: 2 * 60 * 1000 });
      collector.on('collect', async (reaction, user) => {
        try {
          const name = reaction.emoji.name;
          if (name === 'ℹ️') {
            // Decide which database to use based on artist detection
            const { artist, title } = guessArtistAndTitleForCurrent();
            const a = (artist || '').toLowerCase();
            const q = title || currentSong?.title || '';
            if (a.includes('uzi') || a.includes('lil uzi') || a.includes('symere')) {
              await handleUziInfoQuery(message, q);
            } else {
              await handleSongInfoQuery(message, q);
            }
          } else if (name === '⏭️') {
            // Skip current track silently
            if (connection && isPlaying) {
              // When next track starts, update the embed in-place
              try {
                player.once(AudioPlayerStatus.Playing, async () => {
                  try {
                    const payload = await buildNowPlayingMinimalPayload();
                    await replyMsg.edit(payload);
                  } catch {}
                });
              } catch {}
              try { player.stop(); } catch {}
            }
          } else if (name === '⏮️') {
            // Restart current track (enqueue same track at front, then stop)
            if (currentSong) {
              try {
                if (currentSong.type === 'file' && currentSong.path) {
                  songQueue.unshift({ type: 'file', path: currentSong.path, title: currentSong.title, source: currentSong.source || 'local' });
                } else if (currentSong.type === 'youtube' && currentSong.url) {
                  songQueue.unshift({ type: 'youtube', url: currentSong.url, title: currentSong.title, source: currentSong.source || 'youtube' });
                }
                if (connection && isPlaying) {
                  try {
                    player.once(AudioPlayerStatus.Playing, async () => {
                      try {
                        const payload = await buildNowPlayingMinimalPayload();
                        await replyMsg.edit(payload);
                      } catch {}
                    });
                  } catch {}
                  try { player.stop(); } catch {}
                }
              } catch {}
            }
          }
          // Try to remove user's reaction so they can press again
          try { await reaction.users.remove(user.id); } catch {}
        } catch {}
      });
      collector.on('end', async () => {
        try { await replyMsg.reactions.removeAll(); } catch {}
      });
    } catch {}
  } catch (e) {
    console.error('nowplaying error:', e);
    try { await message.reply(`🎶 Now playing: ${currentSong.title || 'Unknown'}`); } catch {}
  }
}

async function handleLastfmNowPlaying(message, username) {
  try {
    if (!username) return void message.reply('❌ Usage: !lastfm <username>');
    if (!LASTFM_API_KEY) return void message.reply('❌ LASTFM_API_KEY missing. Add it to your .env or config.json and restart.');
    const url = `https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=${encodeURIComponent(username)}&api_key=${encodeURIComponent(LASTFM_API_KEY)}&format=json&limit=1`;
    const res = await fetch(url);
    if (!res.ok) return void message.reply('❌ Failed to reach Last.fm.');
    const data = await res.json();
    const tracks = data?.recenttracks?.track;
    const arr = Array.isArray(tracks) ? tracks : (tracks ? [tracks] : []);
    if (arr.length === 0) return void message.reply(`ℹ️ No recent scrobbles for ${username}.`);
    const t = arr[0] || {};
    const name = t.name || 'Unknown Title';
    const artist = (t.artist && (t.artist['#text'] || t.artist.name)) || 'Unknown Artist';
    const album = (t.album && t.album['#text']) || '';
    const trackUrl = t.url || `https://www.last.fm/user/${encodeURIComponent(username)}`;
    const imgArr = Array.isArray(t.image) ? t.image : [];
    const pickImg = (sizes) => {
      for (const sz of sizes) {
        const f = imgArr.find(i => i.size === sz && i['#text']);
        if (f && f['#text']) return f['#text'];
      }
      return '';
    };
    const artUrl = pickImg(['extralarge','large','medium']);
    const now = t['@attr'] && t['@attr'].nowplaying === 'true';
    const whenText = (() => {
      if (now) return 'Now playing';
      const uts = Number(t?.date?.uts);
      if (!uts) return t?.date?.['#text'] || '';
      const diff = Math.max(0, Math.floor((Date.now() - uts * 1000) / 1000));
      const mins = Math.floor(diff / 60), hrs = Math.floor(mins / 60), days = Math.floor(hrs / 24);
      if (days > 0) return `${days}d ${hrs % 24}h ago`;
      if (hrs > 0) return `${hrs}h ${mins % 60}m ago`;
      if (mins > 0) return `${mins}m ago`;
      return `${diff}s ago`;
    })();

    // Fetch extras: user's total scrobbles and per-track user playcount
    let totalScrobbles = null;
    let userTrackPlays = null;
    try {
      const uUrl = `https://ws.audioscrobbler.com/2.0/?method=user.getinfo&user=${encodeURIComponent(username)}&api_key=${encodeURIComponent(LASTFM_API_KEY)}&format=json`;
      const tiUrl = `https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=${encodeURIComponent(LASTFM_API_KEY)}&artist=${encodeURIComponent(artist)}&track=${encodeURIComponent(name)}&username=${encodeURIComponent(username)}&autocorrect=1&format=json`;
      const [uRes, tiRes] = await Promise.all([fetch(uUrl).catch(() => null), fetch(tiUrl).catch(() => null)]);
      if (uRes && uRes.ok) {
        const uj = await uRes.json();
        const pc = uj?.user?.playcount;
        if (pc != null) totalScrobbles = Number(pc);
      }
      if (tiRes && tiRes.ok) {
        const tj = await tiRes.json();
        const up = tj?.track?.userplaycount;
        if (up != null) userTrackPlays = Number(up);
      }
    } catch {}

    const embed = new Discord.EmbedBuilder()
      .setTitle(`${now ? '🎧 Now playing' : '🎵 Last played'} — ${username}`)
      .setURL(`https://www.last.fm/user/${encodeURIComponent(username)}`)
      .setDescription(`[${name}](${trackUrl})`)
      .setColor(now ? 0x1DB954 : 0x7289DA)
      .addFields(
        { name: 'Artist', value: artist.slice(0, 1024), inline: true },
        { name: 'Album', value: (album || '—').slice(0, 1024), inline: true },
        ...(userTrackPlays != null ? [{ name: 'Your Plays', value: userTrackPlays.toLocaleString?.('en-US') || String(userTrackPlays), inline: true }] : []),
        ...(totalScrobbles != null ? [{ name: 'Total Scrobbles', value: totalScrobbles.toLocaleString?.('en-US') || String(totalScrobbles), inline: true }] : [])
      )
      .setFooter({ text: whenText });
    if (artUrl) embed.setThumbnail(artUrl);
    return void message.reply({ embeds: [embed] });
  } catch (e) {
    console.error('Last.fm error:', e);
    return void message.reply('❌ Error fetching Last.fm data.');
  }
}

async function handleCoverDebugCommand(message, query) {
  try {
    let song = null;
    if (query) song = findSheetSongByName(query);
    if (!song && isPlaying && currentSong) song = findSheetSongByName(currentSong.title || '') || { name: currentSong.title, type: 'released' };
    if (!song && query) song = { name: query, type: 'released' };
    if (!song) return void message.reply('❌ Provide a name or play something first.');
    let chosen = findCoverForSong(song);
    if (!chosen && song.type !== 'unreleased') {
      const alt = { ...song, type: 'unreleased' };
      chosen = findCoverForSong(alt);
      if (chosen) song = alt;
    }
    const payload = buildEmbedPayload(song);
    await message.reply(payload);
    const pathText = (payload.files && payload.files[0]?.attachment) ? String(payload.files[0].attachment) : 'none';
    await message.channel.send(`🖼️ Cover path: ${pathText}\nType used: ${song.type || 'unknown'}`);
  } catch (e) {
    console.error('coverdebug error:', e);
    try { await message.reply('❌ coverdebug failed. Check console.'); } catch {}
  }
}

async function handleLyricsCommand(message, query) {
  // Resolve title and artist from query or current song so 'artist' is non-empty
  let input = query && query.trim();
  if (!input && isPlaying && currentSong) input = currentSong.title;
  if (!input) return message.reply('❌ No song specified and nothing is playing.');

  // Try to derive artist/title
  let title = '';
  let artist = '';
  const splitOnDash = (s) => {
    const idx = s.indexOf(' - ');
    if (idx > 0) return { artist: s.slice(0, idx).trim(), title: s.slice(idx + 3).trim() };
    return null;
  };

  // 1) If the user typed "Artist - Title"
  const fromQuery = input ? splitOnDash(input) : null;
  if (fromQuery) { artist = fromQuery.artist; title = fromQuery.title; }

  // 2) If playing a local file, prefer metadata from SONG_META
  if ((!artist || !title) && isPlaying && currentSong && currentSong.type === 'file' && currentSong.path) {
    try {
      const fname = path.basename(currentSong.path);
      const meta = SONG_META[fname];
      if (meta) {
        if (!artist && meta.artist && meta.artist !== 'Unknown Artist') artist = meta.artist;
        if (!title && meta.title) title = meta.title;
      }
    } catch {}
  }

  // 3) If still missing, attempt to split the display title
  if ((!artist || !title) && isPlaying && currentSong && currentSong.title) {
    const fromPlaying = splitOnDash(currentSong.title);
    if (fromPlaying) {
      if (!artist) artist = fromPlaying.artist;
      if (!title) title = fromPlaying.title;
    }
  }

  // 4) As a last resort, assume Juice WRLD context and treat input as the title
  if (!title) title = input;
  if (!artist) artist = 'Juice WRLD';

  if (!GENIUS_API_TOKEN) {
    return message.reply('❌ Genius API token missing. Add GENIUS_API_TOKEN to your .env (no quotes) and restart.');
  }

  const options = { apiKey: GENIUS_API_TOKEN, title, artist, optimizeQuery: true };
  try {
    const lyrics = await getLyrics(options);
    if (!lyrics) return message.reply('❌ Lyrics not found.');
    for (let i = 0; i < lyrics.length; i += 2000) await message.reply(lyrics.substring(i, i + 2000));
  } catch (err) {
    console.error('Lyrics error:', err);
    // Retry once swapping parsed roles if the query looked reversed (Title - Artist)
    if (splitOnDash(input) == null) {
      const maybeReversed = input && input.includes(' - ');
      if (maybeReversed) {
        try {
          const parts = input.split(' - ');
          const retry = { apiKey: GENIUS_API_TOKEN, title: parts[0].trim(), artist: parts.slice(1).join(' - ').trim(), optimizeQuery: true };
          const alt = await getLyrics(retry);
          if (alt) {
            for (let i = 0; i < alt.length; i += 2000) await message.reply(alt.substring(i, i + 2000));
            return;
          }
        } catch {}
      }
    }
    message.reply('❌ Error fetching lyrics.');
  }
}

// ================== GOOGLE SHEETS ==================
async function handleSongInfoQuery(message, rawQuery) {
  const q = (rawQuery || '').trim();
  if (!q) return void message.reply('❌ Please specify a song name. Example: `!songinfo lucid dreams`');

  const normQuery = normalizeStr(q);
  const candidates = [];
  for (const [key, song] of Object.entries(sheetSongLibrary)) {
    const hay = [key, song.name || '', (song.aliases || []).join(' '), song.project || '', song.version || '', song.additionalInfo || '', song.era || ''].join(' ');
    const normHay = normalizeStr(hay);
    if (normHay.includes(normQuery)) candidates.push(song);
  }

  let matches = candidates;
  if (matches.length === 0) {
    const scored = [];
    for (const [key, song] of Object.entries(sheetSongLibrary)) {
      const hay = [key, song.name || '', (song.aliases || []).join(' '), song.project || '', song.version || '', song.additionalInfo || '', song.era || ''].join(' ');
      const score = tokenOverlapScore(hay, q);
      if (score > 0) scored.push({ song, score });
    }
    scored.sort((a, b) => b.score - a.score);
    matches = scored.slice(0, 50).map(s => s.song);
  }

  if (matches.length === 0) return void message.reply(`❌ No songs found matching "${q}" in the Juice WRLD database.`);

  // Include duplicates (same song name across versions/categories) in the dropdown
  const allSongs = Object.values(sheetSongLibrary);
  const canonicalNameKey = (name) => {
    const s = String(name || '');
    // strip trailing bracketed tokens like [v2], [demo], [alt]
    let t = s.replace(/\s*\[(?:v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\]$/i, '');
    // remove (feat. ...), (with ...)
    t = t.replace(/\s*\((?:feat\.|with)[^)]+\)/ig, '');
    // drop version/variant tokens inline
    t = t.replace(/\b(v(?:er(?:sion)?)?\s*\d+|remix|demo|alt|alternate|instrumental|acapella|live|snippet|snip|extended|clean|explicit|no\s*hook)\b/ig, '');
    // normalize whitespace and punctuation
    t = t.replace(/[^a-z0-9]+/ig, ' ').trim().replace(/\s+/g, ' ');
    return t.toLowerCase();
  };
  const nameKey = (s) => canonicalNameKey(s?.name || '');
  const makeId = (s) => (s && s._key) ? String(s._key) : [s.name, s.project, s.version, s.category, s.type].map(x => String(x || '')).join('|').toLowerCase();

  let expanded = [];
  const seen = new Set();
  const add = (s) => { const id = makeId(s); if (!seen.has(id)) { seen.add(id); expanded.push(s); } };

  if (matches.length === 1) {
    // If only one match by query, gather all entries that share the same normalized name
    const k = nameKey(matches[0]);
    const group = allSongs.filter(s => nameKey(s) === k);
    for (const s of group.length > 0 ? group : matches) add(s);
  } else {
    // For multiple matches, union each match with all entries that share its normalized name
    for (const m of matches) {
      const k = nameKey(m);
      const group = allSongs.filter(s => nameKey(s) === k);
      if (group.length > 0) { for (const s of group) add(s); }
      else add(m);
    }
  }

  // Also include and prioritize entries whose NAME or ALIASES match the query exactly (normalized)
  const qKey = canonicalNameKey(q);
  if (qKey) {
    const exactByName = allSongs.filter(s => {
      if (nameKey(s) === qKey) return true;
      const aliases = Array.isArray(s.aliases) ? s.aliases : [];
      return aliases.some(a => canonicalNameKey(a) === qKey);
    });
    if (exactByName.length > 0) {
      // put exact name matches first
      const prioritized = [];
      const seen2 = new Set();
      const pushU = (s) => { const id = makeId(s); if (!seen2.has(id)) { seen2.add(id); prioritized.push(s); } };
      for (const s of exactByName) pushU(s);
      for (const s of expanded) pushU(s);
      expanded = prioritized;
    }
  }

  if (expanded.length === 1) {
    // Truly a single version; just show it
    return void message.channel.send(buildEmbedPayload(expanded[0]));
  }
  // Stable sort: released -> unreleased -> unsurfaced, then project, then version
  const typeOrder = { released: 0, unreleased: 1, unsurfaced: 2 };
  expanded.sort((a, b) => {
    const ta = typeOrder[a.type] ?? 99; const tb = typeOrder[b.type] ?? 99;
    if (ta !== tb) return ta - tb;
    const pa = (a.project || '').toLowerCase(); const pb = (b.project || '').toLowerCase();
    if (pa !== pb) return pa.localeCompare(pb);
    const va = (a.version || '').toLowerCase(); const vb = (b.version || '').toLowerCase();
    return va.localeCompare(vb);
  });

  const MAX_OPTIONS = 25;
  const token = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const limited = expanded.slice(0, MAX_OPTIONS);
  songInfoInteractions.set(token, limited);

  // Build name counts to detect duplicates and disambiguate labels
  const nameCounts = limited.reduce((acc, s) => {
    const k = s.name || '';
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});

  const options = limited.map((s, idx) => {
    let label = `${s.name}`;
    if (nameCounts[s.name] > 1) {
      const variant = s.version || (s.type === 'released' ? (s.project || 'Project') : (s.era || 'Era')) || s.category || '';
      if (variant) label = `${s.name} — ${variant}`;
    }
    label = label.slice(0, 100);
    const verPrefix = s.version && !s.name.toLowerCase().includes(s.version.toLowerCase()) ? `${s.version} • ` : '';
    const info = s.type === 'released' ? (s.project || 'Unknown Project') : (s.era || 'Unknown Era');
    const desc = `${verPrefix}${info} • ${s.category}`.slice(0, 100);
    return { label, description: desc, value: String(idx) };
  });

  const select = new Discord.StringSelectMenuBuilder().setCustomId(`songinfo_select:${token}`).setPlaceholder('Select a version').addOptions(options);
  const row = new Discord.ActionRowBuilder().addComponents(select);

  // If all options share the same normalized name, present that as the base title
  const allSameName = limited.length > 0 && limited.every(s => nameKey(s) === nameKey(limited[0]));
  const baseName = allSameName ? (limited[0]?.name || q) : q;
  const listPreview = limited.map((s, i) => {
    const categoryEmoji = s.type === 'released' ? '🟢' : s.type === 'unreleased' ? '🟡' : '🟣';
    const verPrefix = s.version && !s.name.toLowerCase().includes(s.version.toLowerCase()) ? `${s.version} • ` : '';
    const info = s.type === 'released' ? (s.project || 'Unknown Project') : (s.era || 'Unknown Era');
    return `**${i + 1}. ${s.name}**\n${categoryEmoji} ${verPrefix}${info} • ${s.category}`;
  }).join('\n\n');

  const total = expanded.length;
  const embed = new Discord.EmbedBuilder().setTitle(`🎵 Select a version: ${baseName}`).setDescription(`${listPreview}${total > MAX_OPTIONS ? `\n\n...and ${total - MAX_OPTIONS} more. Refine your search to see all.` : ''}`).setColor('#5865F2');
  return void message.reply({ embeds: [embed], components: [row] });
}
async function getSheetsClient() {
  try {
    let credentials = null;
    const credPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    if (credPath) {
      try {
        // Prefer path to JSON file
        if (fs.existsSync(credPath)) {
          const raw = fs.readFileSync(credPath, 'utf8');
          credentials = JSON.parse(raw);
        } else {
          // If the env contains raw JSON content, parse it
          try { credentials = JSON.parse(credPath); } catch {}
        }
      } catch {}
    }
    // Fallback: read inline credentials from env
    if (!credentials) {
      const email = process.env.GOOGLE_CLIENT_EMAIL || process.env.GOOGLE_SERVICE_EMAIL;
      let key = process.env.GOOGLE_PRIVATE_KEY || process.env.GOOGLE_SERVICE_PRIVATE_KEY;
      if (key && key.includes('\\n')) key = key.replace(/\\n/g, '\n');
      if (email && key) credentials = { client_email: email, private_key: key };
    }
    if (!credentials) {
      console.error('❌ Google credentials not configured. Set GOOGLE_APPLICATION_CREDENTIALS to a JSON file path, or GOOGLE_CLIENT_EMAIL and GOOGLE_PRIVATE_KEY in .env');
      return null;
    }
    const auth = new JWT({ email: credentials.client_email, key: credentials.private_key, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    return google.sheets({ version: 'v4', auth });
  } catch (error) {
    console.error('❌ Failed to create Sheets client:', error.message || error);
    return null;
  }
}

async function loadSongInfoFromSheets() {
  try {
    const sheetsClient = await getSheetsClient();
    if (!sheetsClient) { console.log('⚠️ Google Sheets not available'); return; }
    console.log('📊 Loading song info from Google Sheets...');
    sheetSongLibrary = {};
    const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: GOOGLE_SHEETS_CONFIG.SHEET_ID });
    const titles = (meta.data.sheets || []).map(s => s.properties.title);
    const targetTitles = titles.filter(t => /discography/i.test(t) || /(released|unreleased|unsurfaced)/i.test(t));
    if (targetTitles.length === 0) { console.warn('⚠️ No matching tabs found.'); return; }
    const ranges = targetTitles.map(t => `'${t.replace(/'/g, "''")}'!A1:AG40000`);
    const resp = await sheetsClient.spreadsheets.values.batchGet({ spreadsheetId: GOOGLE_SHEETS_CONFIG.SHEET_ID, ranges });
    let totalSongs = 0; let compositeDupes = 0; let nameDupes = 0; const seenNames = new Set();
    const parseSheet = (values, sheetName) => {
      if (!values || values.length === 0) return 0;
      const hdr = _findHeaderIndices(values);
    try { console.log(`[sheets] Header indices for "${sheetName}":`, hdr); } catch {}
      const startRow = hdr.headerRow >= 0 ? hdr.headerRow + 1 : 0;
      let currentProject = 'Unknown Project'; let currentEra = ''; let count = 0;
      const extractAliases = ({ baseName, info, fileMeta }) => {
        const out = new Set();
        const add = (s) => {
          if (!s) return; const t = String(s).trim(); if (!t) return;
          if (t.length < 3) return; // avoid tiny tokens
          if (t.toLowerCase() === String(baseName || '').toLowerCase()) return;
          out.add(t);
        };
        // From Additional Info: aka/also known as/formerly/originally/working title
        const srcs = [info].filter(Boolean);
        for (const src of srcs) {
          const text = String(src);
          const re = /(aka|a\.k\.a\.|also known as|formerly titled|originally titled|working title|alt name)[:\s-]*\"?([^\"\n\r;|,]+)\"?/gi;
          let m; while ((m = re.exec(text)) !== null) { add(m[2]); }
          // quoted titles often indicate alternate labels
          const qre = /"([^"]{3,80})"/g; let qm; while ((qm = qre.exec(text)) !== null) { add(qm[1]); }
        }
        // From File Meta: split by separators and strip noise
        if (fileMeta) {
          const cleaned = String(fileMeta).replace(/\.(mp3|wav|flac|m4a|ogg)$/i, '');
          const parts = cleaned.split(/[\/|]|\s{2,}|\s-\s/).map(s => s && s.toString().trim()).filter(Boolean);
          for (let p of parts) {
            // remove version/noise tokens
            p = p.replace(/\b(v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\b/ig, '').replace(/\s{2,}/g, ' ').trim();
            if (p) add(p);
          }
        }
        return Array.from(out);
      };
      const parseSurfaceDate = (raw) => {
        if (!raw) return '';
        let s = String(raw).trim();
        // remove leading labels and line breaks like 'Surfaced\nJune 3, 2022'
        s = s.replace(/^surfaced\s*/i, '');
        // if multi-line, take the last non-empty line
        const parts = s.split(/\r?\n/).map(t => t.trim()).filter(Boolean);
        if (parts.length > 1) s = parts[parts.length - 1];
        // remove trailing dots and normalize spaces
        s = s.replace(/\.+$/,'').replace(/\s{2,}/g, ' ').trim();
        return s;
      };
      const parseTitleAndAliases = (raw) => {
        const res = { title: '', aliases: [] };
        if (!raw) return res;
        const lines = String(raw).split(/\r?\n/).map(t => t.trim()).filter(Boolean);
        if (lines.length === 0) return res;
        // First non-empty line is the primary title
        let primary = lines[0];
        // strip bracketed version tokens from display title only
        primary = primary.replace(/\s*\[(?:v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\]$/i, '').trim();
        res.title = primary;
        const addAlias = (s) => {
          if (!s) return; const t = s.trim(); if (!t) return; if (t.length < 3) return;
          if (t.toLowerCase() === primary.toLowerCase()) return;
          res.aliases.push(t);
        };
        const sepRe = /\s*[|/•·]+\s*/;
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].replace(/^[-–—]\s*/, '');
          const parts = line.split(sepRe).map(x => x.trim()).filter(Boolean);
          for (let p of parts) {
            // remove common annotations
            p = p.replace(/\s*\((?:feat\.|with)[^)]+\)/ig, '').trim();
            p = p.replace(/\s*\[(?:v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\]$/i, '').trim();
            if (p) addAlias(p);
          }
        }
        // dedupe
        res.aliases = Array.from(new Set(res.aliases));
        return res;
      };
      for (let r = startRow; r < values.length; r++) {
        const row = values[r] || [];
        const cell = (i) => (i >= 0 && row[i] != null ? String(row[i]).trim() : '');
        const bannerEra = _detectEraFromRowCells(row);
        if (bannerEra && !/\breleased\b/i.test(sheetName)) currentEra = bannerEra;
        const projectRaw = cell(hdr.projectIdx); if (projectRaw) currentProject = projectRaw;
  let songTitleRaw = cell(hdr.titleIdx);
  const ta = parseTitleAndAliases(songTitleRaw);
  let songTitle = ta.title;
        if (!_looksLikeTitle(songTitle)) {
          const colA = (row[0] || '').toString().trim();
          if ((/misc|unsurfaced/i.test(sheetName)) && _looksLikeTitle(colA) && colA !== currentProject) songTitle = colA;
        }
        if (!_looksLikeTitle(songTitle)) continue;
  songTitle = songTitle.replace(/\s+/g, ' ').trim();
        const artist = cell(hdr.artistIdx);
        const producer = cell(hdr.producerIdx);
        const engineer = cell(hdr.engineerIdx);
        const location = cell(hdr.locationIdx);
        const recordDate = cell(hdr.recordDateIdx);
        const releaseDate = cell(hdr.releaseDateIdx);
  const surfaceDate = parseSurfaceDate(cell(hdr.surfaceDateIdx));
        const info = cell(hdr.infoIdx);
        const v1 = cell(hdr.version1Idx);
        const v2 = cell(hdr.version2Idx);
        const categoryCell = cell(hdr.categoryIdx);
        const instrumentalName = cell(hdr.instrumentalIdx);
        const fileMeta = cell(hdr.fileMetaIdx);
        const previewDate = cell(hdr.previewDateIdx);
        const duration = cell(hdr.durationIdx);
        const properties = cell(hdr.propertiesIdx);
        const versionTag = detectVersionFromRow({ name: songTitle, colJ: v1, colK: v2, info });
  const versionKey = normalizeVersionTag(versionTag) || 'default';
  const nameKey = songTitle.toLowerCase();
  const compositeKey = `${nameKey}|${sheetName.toLowerCase()}|${(currentProject || '').toLowerCase()}|${versionKey}`;
  if (seenNames.has(nameKey)) nameDupes++; else seenNames.add(nameKey);
        const sheetLC = (sheetName || '').toLowerCase();
        const catLC = (categoryCell || '').toLowerCase();
        let type = 'released';
        if (/\bunsurfaced\b/.test(sheetLC) || /\bunsurfaced\b/.test(catLC)) {
          type = 'unsurfaced';
        } else if (/\bunreleased\b/.test(sheetLC) || /\bunreleased\b/.test(catLC) || /(\bleak|\bleaked|\bsurfaced)/.test(sheetLC) || /(\bleak|\bleaked|\bsurfaced)/.test(catLC)) {
          type = 'unreleased';
        }
  const songData = { name: songTitle, project: currentProject || 'Unknown Project', artist: artist || 'Juice WRLD', producer: producer || 'Unknown Producer', engineer: engineer || 'Unknown Engineer', recordingLocation: location || '', recordDate: recordDate || '', additionalInfo: info || '', version: versionTag || '', category: sheetName, type, sheetSource: 'sheets', instrumentalName: instrumentalName || '', fileMeta: fileMeta || '', previewDate: previewDate || '', duration: duration || '', properties: properties || '' };
        try {
          const a1 = extractAliases({ baseName: songTitle, info, fileMeta });
          const a2 = (ta.aliases || []);
          songData.aliases = Array.from(new Set([...(a1 || []), ...a2]));
        } catch { songData.aliases = ta.aliases || []; }
  if (type === 'released' && releaseDate) songData.releaseDate = releaseDate;
  else if (type === 'unreleased' && surfaceDate) { songData.surfaceDate = surfaceDate; try { console.log(`[sheets] Surface date set for ${songTitle}: ${surfaceDate}`); } catch {} }
        if (type !== 'released' && currentEra) songData.era = currentEra;
        // Keep duplicates instead of dropping them: uniquify the key
        let uniqueKey = compositeKey;
        if (sheetSongLibrary[uniqueKey]) {
          compositeDupes++;
          let idx = 2;
          while (sheetSongLibrary[`${compositeKey}#${idx}`]) idx++;
          uniqueKey = `${compositeKey}#${idx}`;
        }
  // Attach internal unique key so downstream grouping can distinguish identical-metadata duplicates
  songData._key = uniqueKey;
  sheetSongLibrary[uniqueKey] = songData; count++;
      }
      return count;
    };
    const valueRanges = resp.data.valueRanges || [];
    for (let i = 0; i < valueRanges.length; i++) {
      const vr = valueRanges[i];
      const m = /'([^']+)'!/i.exec(vr.range || '');
      const sheetName = (m && m[1]) || targetTitles[i] || 'Unknown';
      const added = parseSheet(vr.values, sheetName);
      console.log(`✅ Found ${added} entries in "${sheetName}"`);
      totalSongs += added;
    }
    const sample = Object.values(sheetSongLibrary).slice(0, 5).map(s => `${s.name}${s.version ? ` (${s.version})` : ''} • ${s.project}`);
    console.log('🔎 Sample entries:', sample);
    console.log(`✅ Total entries loaded: ${totalSongs}`);
  console.log(`ℹ️ Duplicates preserved by composite key: ${compositeDupes}`);
    console.log(`ℹ️ Same-name collisions observed: ${nameDupes}`);
  } catch (error) {
    console.error('❌ Error loading from Google Sheets:', error.message || error);
  }
}

function normalizeVersionTag(tag) {
  if (!tag) return '';
  const t = tag.toLowerCase().trim().replace(/\s+/g, ' ');
  const num = t.match(/\b(v|ver|version)\s*([0-9]+)\b/);
  if (num) return `v${num[2]}`;
  const known = ['demo','alt','alternate','og','original','cdq','extended','clean','explicit','no hook','instrumental','acapella','remix','live','studio','snippet','snip'];
  for (const k of known) {
    if (t.includes(k)) {
      if (k === 'alternate') return 'alt';
      if (k === 'original') return 'og';
      if (k === 'snip') return 'snippet';
      return k;
    }
  }
  return t;
}

function detectVersionFromRow({ name, colJ, colK, info }) {
  const parts = [];
  const searchSpace = [colJ, colK, info, name].filter(Boolean).join(' ');
  const mNum = searchSpace.match(/\b(v|ver|version)\s*([0-9]+)\b/i);
  if (mNum) parts.push(`v${mNum[2]}`);
  const tokens = ['demo','alt','alternate','og','original','cdq','extended','clean','explicit','no hook','instrumental','acapella','remix','live','studio','snippet','snip'];
  for (const tok of tokens) {
    const re = new RegExp(`\\b${tok.replace(' ', '\\s+')}\\b`, 'i');
    if (re.test(searchSpace)) parts.push(tok);
  }
  const normalized = [...new Set(parts.map(normalizeVersionTag).filter(Boolean))];
  return normalized.join(' ');
}

function _normHeader(s) { return (s || '').toString().trim().toLowerCase().replace(/\s+/g, ' '); }
function _findHeaderIndices(values) {
  const maxScan = Math.min(200, values.length);
  let headerRow = -1; let headers = [];
  for (let r = 0; r < maxScan; r++) {
    const row = values[r] || [];
    const norm = row.map(_normHeader);
    const score = (norm.some(c => /(song|title|track)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(project|album|mixtape)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(credited artist|artist)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(producer|prod)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(engineer)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(additional info|notes|information)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(release|surface|leak).*(date)/.test(c)) ? 1 : 0)
      + (norm.some(c => /(category)/.test(c)) ? 1 : 0);
    if (score >= 2) { headerRow = r; headers = norm; break; }
  }
  const idx = (names) => {
    for (let i = 0; i < headers.length; i++) { for (const n of names) if (headers[i] === n) return i; for (const n of names) if (headers[i]?.includes(n)) return i; }
    return -1;
  };
  const idxExact = (names) => {
    for (let i = 0; i < headers.length; i++) { for (const n of names) if (headers[i] === n) return i; }
    return -1;
  };
  const titleIdx = idx(['track title(s)', 'song', 'title', 'track']);
  const projectIdx = idx(['project', 'album', 'mixtape']);
  const artistIdx = idx(['credited artist(s)', 'artist']);
  const producerIdx = idx(['producer(s)', 'producer', 'prod']);
  const engineerIdx = idx(['engineer(s)', 'engineer']);
  const locationIdx = idx(['recording location(s)', 'location', 'studio']);
  const recordDateIdx = idx(['record date(s)', 'recording date', 'date recorded']);
  const releaseDateIdx = idx(['release date', 'released']);
  let surfaceDateIdx = idx(['surface date', 'surfaced', 'leak date', 'leaked date', 'date leaked', 'first surfaced', 'first leaked', 'date surfaced']);
  if (surfaceDateIdx === -1) {
  const genericDateCol = idxExact(['date(s).', 'date(s):', 'date(s)']);
    if (genericDateCol !== -1) surfaceDateIdx = genericDateCol;
    if (surfaceDateIdx === -1) {
      // As a last resort, look for a column named exactly 'date' or containing 'date' but not 'release' or 'record'
      const genericIdx = idxExact(['date']) !== -1 ? idxExact(['date']) : idx(['date']);
      if (genericIdx !== -1 && genericIdx !== releaseDateIdx && genericIdx !== recordDateIdx) surfaceDateIdx = genericIdx;
    }
  }
  const categoryIdx = idx(['category']);
  const version1Idx = idx(['version', 'variant']);
  const version2Idx = idx(['source', 'leak source', 'properties']);
  const infoIdx = idx(['additional info', 'notes', 'information', 'notes/info']);
  const fileMetaIdx = idx(['file name(s) | metadata', 'file name(s)/metadata', 'file name(s)', 'file name', 'files', 'metadata']);
  const instrumentalIdx = idx(['instrumental name(s)', 'instrumental name']);
  const previewDateIdx = idx(['preview date', 'first previewed', 'first teased']);
  const durationIdx = idx(['duration', 'length']);
  const propertiesIdx = idx(['properties']);
  return { headerRow, titleIdx, projectIdx, artistIdx, producerIdx, engineerIdx, locationIdx, recordDateIdx, releaseDateIdx, surfaceDateIdx, categoryIdx, version1Idx, version2Idx, infoIdx, fileMetaIdx, instrumentalIdx, previewDateIdx, durationIdx, propertiesIdx };
}
function _looksLikeTitle(s) {
  if (!s) return false; const t = String(s).trim(); if (t.length < 2) return false; if (/^(please|era\b|project\b|tracks?\b|copyright|suggestion|song\s*name|album|discography)$/i.test(t)) return false; if (/^\d+$/.test(t)) return false; if (/^n\/?a$/i.test(t)) return false; return true;
}

function _detectEraFromRowCells(row) {
  try {
    const raw = (row || []).map(v => (v == null ? '' : String(v))).join(' ').trim();
    if (!raw) return '';
    const isMeaningfulLabel = (label) => {
      if (!label) return false;
      const s = label.toString().trim();
      if (s.length < 3) return false;
      const n = s.toLowerCase();
      const generic = new Set(['track','tracks','title','titles','project','projects','category','categories','information','notes','misc','discography','unknown','n/a','na']);
      return !generic.has(n);
    };
  // Pattern: "— Some Name (Sessions)" or "Some Name (Sessions)" -> Normalize to "Some Name Sessions"
  const mSess = raw.match(/.*[\u2013\u2014-]\s*([^\u2013\u2014-]+?)\s*\(sessions\)/i) || raw.match(/\b([^()]+?)\s*\(sessions\)/i);
    if (mSess && mSess[1]) {
      const name = mSess[1].replace(/["'()]/g, '').trim();
      if (!isMeaningfulLabel(name)) return '';
      return /sessions$/i.test(name) ? name : `${name} Sessions`;
    }
    // Prefer named eras (e.g., "Goodbye & Good Riddance Era" or "Era: Outsiders")
    // Pattern 1: "<Name> Era"
    const mNameBefore = raw.match(/\b([A-Za-z][A-Za-z0-9 &'!.-]{2,})\s+Era\b/i);
    if (mNameBefore && mNameBefore[1]) {
      const label = mNameBefore[1].trim();
      if (isMeaningfulLabel(label)) return `${label} Era`;
    }
    // Pattern 2: "Era: <Name>" or "Era - <Name>"
    const mNameAfter = raw.match(/\bEra\s*[:#-]\s*([A-Za-z][A-Za-z0-9 &'!.-]{2,})\b/i);
    if (mNameAfter && mNameAfter[1]) {
      const label = mNameAfter[1].trim();
      if (isMeaningfulLabel(label)) return `${label} Era`;
    }
    // If only a numeric ERA banner is present (e.g., ERA11), do not synthesize a number-based era label
    return '';
  } catch { return ''; }
}

// ================== VOICE COMMANDS ==================
async function listenForVoiceCommands(connection, message) {
  if (!voiceRecognitionEnabled) return;
  const receiver = connection.receiver;
  let waitingForWake = true;
  let wakeTimeout = null;
  let pendingSong = null;
  receiver.speaking.on('start', (userId) => {
    if (autoVolumeEnabled && player.state.resource && player.state.resource.volume && !loweredForSpeech) {
      if (client.user && userId !== client.user.id) {
        lastVolume = player.state.resource.volume.volume;
        player.state.resource.volume.setVolume(0.2);
        loweredForSpeech = true;
      }
    }
    if (userId !== message.author.id) return;
    if (receiver._listening) return;
    receiver._listening = true;
    const opusStream = receiver.subscribe(userId, { end: { behavior: EndBehaviorType.AfterSilence, duration: 1000 } });
    const pcmStream = new prism.opus.Decoder({ rate: 16000, channels: 1, frameSize: 960 });
    opusStream.pipe(pcmStream);
    const recognizeStream = speechClient.streamingRecognize({ config: { encoding: 'LINEAR16', sampleRateHertz: 16000, languageCode: 'en-US' }, interimResults: false })
      .on('error', (err) => { console.error('Speech API error:', err); receiver._listening = false; })
      .on('data', (data) => {
        const transcript = data?.results?.[0]?.alternatives?.[0]?.transcript?.trim();
        if (!transcript) return;
        const lower = transcript.toLowerCase();
        console.log(`🎤 Heard: "${lower}"`);
        if (pendingSong) {
          if (/\byes\b/.test(lower)) {
            const fakeMessage = { ...message, reply: (c) => message.channel.send(c) };
            const displayName = SONG_LIBRARY[pendingSong] || pendingSong;
            handlePlayCommand(fakeMessage, pendingSong);
            message.channel.send(`✅ Added "${displayName}" to the queue!`);
            pendingSong = null; waitingForWake = true; return;
          }
          if (/\bno\b/.test(lower)) { message.channel.send('❌ Okay, not adding the song.'); pendingSong = null; waitingForWake = true; return; }
          return;
        }
        if (waitingForWake) {
          if (/\b(rise|hey)\b/i.test(lower)) {
            message.channel.send('👂 I\'m listening!');
            waitingForWake = false;
            if (wakeTimeout) clearTimeout(wakeTimeout);
            wakeTimeout = setTimeout(() => { waitingForWake = true; message.channel.send('⌛ Listening timed out. Say "rise" or "hey" to wake me again.'); }, 10000);
          }
          return;
        }
        if (lower.startsWith('play ')) {
          const songName = lower.slice(5).trim();
          const { bestMatch, score } = findBestMatchingSong(songName);
          if (bestMatch && score >= 2) { const displayName = SONG_LIBRARY[bestMatch] || bestMatch; pendingSong = bestMatch; message.channel.send(`❓ Did you mean "${displayName}"? Say "yes" or "no".`); }
          else if (bestMatch && score > 0) { const displayName = SONG_LIBRARY[bestMatch] || bestMatch; pendingSong = bestMatch; message.channel.send(`❓ I couldn't find an exact match. Did you maybe mean "${displayName}"? Say "yes" or "no".`); }
          else { message.channel.send('❌ No matching song found. Try again!'); }
        } else if (/\bskip\b/i.test(lower)) {
          const fakeMessage = { ...message, reply: (c) => message.channel.send(c) }; handleSkipCommand(fakeMessage);
        } else if (/\bresume\b/i.test(lower)) {
          const fakeMessage = { ...message, reply: (c) => message.channel.send(c) }; handleResumeCommand(fakeMessage);
        } else if (/\b(pause|leave|stop)\b/i.test(lower)) {
          const fakeMessage = { ...message, reply: (c) => message.channel.send(c) }; handleStopCommand(fakeMessage);
        } else {
          const m = lower.match(/volume\s+(\d{1,3})/i);
          if (m) {
            const vol = parseInt(m[1], 10);
            if (isNaN(vol) || vol < 0 || vol > 100) message.channel.send('❌ Volume must be between 0 and 100.');
            else { const fakeMessage = { ...message, reply: (c) => message.channel.send(c) }; handleVolumeCommand(fakeMessage, vol); }
          } else {
            message.channel.send('❓ Say "play [song]", "pause", "skip", "resume", or "volume [number]".');
          }
        }
      });
    pcmStream.on('data', (chunk) => { if (!recognizeStream.destroyed && !recognizeStream.writableEnded) recognizeStream.write(chunk); });
    pcmStream.on('end', () => { if (!recognizeStream.destroyed && !recognizeStream.writableEnded) recognizeStream.end(); receiver._listening = false; });
  });
  receiver.speaking.on('end', () => {
    if (autoVolumeEnabled && loweredForSpeech && player.state.resource && player.state.resource.volume) {
      player.state.resource.volume.setVolume(lastVolume || 1.0);
      loweredForSpeech = false;
    }
  });
}

// ================== SEARCH HELPERS ==================
function normalizeStr(s) {
  return (s || '').toString().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');
}
function tokenOverlapScore(a, b) {
  const ta = new Set(normalizeStr(a).split(' ').filter(Boolean));
  const tb = new Set(normalizeStr(b).split(' ').filter(Boolean));
  if (ta.size === 0 || tb.size === 0) return 0;
  let inter = 0; for (const t of ta) if (tb.has(t)) inter++;
  return inter / Math.max(ta.size, tb.size);
}

// ================== EMBED BUILDER ==================
function buildSongEmbed(song) {
  const isMeaningful = (v) => { if (!v) return false; const s = String(v).trim(); return s && !/^(-|n\/a|na|n\.a\.|unknown)$/i.test(s); };
  const push = (arr, name, val, inline = true) => { if (isMeaningful(val)) arr.push({ name, value: String(val).slice(0, 1024), inline }); };
  const cleanFileMetaDisplay = (meta) => { if (!meta) return ''; let s = String(meta); s = s.replace(/\((?:\s*(?:v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\s*)+\)/gi, ''); s = s.replace(/\b(?:v(?:er(?:sion)?)?\s*\d+|alt|instrumental|acapella|remix|demo|extended|clean|explicit|no\s*hook|live|snippet)\b/gi, ''); return s.replace(/\s{2,}/g, ' ').trim(); };
  const fields = [];
  const embed = new Discord.EmbedBuilder().setTitle(`🎵 ${song.name}`).setColor(song.type === 'released' ? '#1DB954' : song.type === 'unreleased' ? '#FF6B35' : '#9146FF');
  const aka = (song.aliases || []).filter(a => a && a.length >= 3 && a.toLowerCase() !== String(song.name || '').toLowerCase());
  if (aka.length > 0) {
    embed.setDescription(`aka: ${aka.slice(0, 5).join(' • ')}`);
  }
  if (song.type === 'released') push(fields, '💿 Project/Album', song.project);
  if ((song.type === 'unreleased' || song.type === 'unsurfaced') && song.era) push(fields, '🕰️ Era', song.era);
  // Version field intentionally omitted from embed
  if (song.artist && song.artist !== 'Juice WRLD') push(fields, '🎤 Artist', song.artist);
  push(fields, '🎛️ Producer(s)', song.producer);
  push(fields, '🔧 Engineer(s)', song.engineer);
  push(fields, '📍 Recording Location(s)', song.recordingLocation);
  if (song.type === 'released') push(fields, '🚀 Release Date', song.releaseDate);
  else if (song.type === 'unreleased') {
    if (song.surfaceDate) push(fields, '🌊 Surface Date', song.surfaceDate);
    if (song.recordDate) push(fields, '🗓️ Record Date', song.recordDate);
  } else {
    // unsurfaced and others: no surface date
    push(fields, '🗓️ Record Date', song.recordDate);
  }
  push(fields, '📄 File Name(s) | Metadata', cleanFileMetaDisplay(song.fileMeta));
  push(fields, '🎼 Instrumental Name(s)', song.instrumentalName);
  push(fields, '👀 Preview Date', song.previewDate);
  push(fields, '⌱ Duration', song.duration);
  push(fields, '🏷️ Properties', song.properties);
  if (isMeaningful(song.additionalInfo)) fields.push({ name: 'ℹ️ Notes', value: String(song.additionalInfo).slice(0, 1024), inline: false });
  push(fields, '📂 Category', song.category);
  if (fields.length > 25) { const core = fields.slice(0, 24); const extra = fields.slice(24).map(f => `• ${f.name}: ${f.value}`).join('\n').slice(0, 1024); core.push({ name: '➕ More', value: extra, inline: false }); embed.addFields(core); }
  else embed.addFields(fields);
  return embed;
}

// ================== COVER ART (THUMBNAILS) ==================
const COVER_DIR = path.join(__dirname, 'covers');
const COVER_DIR_RELEASED = path.join(COVER_DIR, 'released');
const COVER_DIR_UNRELEASED = path.join(COVER_DIR, 'unreleased');
const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp']);

// Optional manual overrides: covers/cover_map.json
let COVER_MAP = { released: {}, unreleased: {}, unsurfaced: {} };
try {
  const cmPath = path.join(COVER_DIR, 'cover_map.json');
  if (fs.existsSync(cmPath)) {
    const raw = fs.readFileSync(cmPath, 'utf8');
    const data = JSON.parse(raw);
    if (data && typeof data === 'object') COVER_MAP = { ...COVER_MAP, ...data };
  }
} catch (e) { console.warn('[covers] Failed to read cover_map.json:', e?.message || e); }

function _norm(s) {
  return (s || '').toString().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');
}

function _listImagesRecursive(dir, depth = 3) {
  const results = [];
  try {
    if (!fs.existsSync(dir)) return results;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory() && depth > 0) {
        results.push(..._listImagesRecursive(p, depth - 1));
      } else if (e.isFile() && IMAGE_EXTS.has(path.extname(e.name).toLowerCase())) {
        results.push(p);
      }
    }
  } catch {}
  return results;
}

function _bestImageByQueries(dir, queries) {
  const files = _listImagesRecursive(dir);
  if (files.length === 0) return null;
  const normQueries = (queries || [])
    .map(q => (typeof q === 'string' ? { q: _norm(q), w: 1 } : { q: _norm(q.q), w: q.w || 1 }))
    .filter(obj => !!obj.q);
  if (normQueries.length === 0) return null;
  // Exact match pass: filename base or any folder segment equals an alias
  for (const file of files) {
    const base = path.basename(file, path.extname(file));
    const nbase = _norm(base);
    const segs = path.dirname(file).split(path.sep).map(_norm).filter(Boolean);
    for (const { q } of normQueries) {
      if (!q) continue;
      if (nbase === q || segs.includes(q)) {
        try { console.log(`[covers] Exact match on "${q}" for ${file}`); } catch {}
        return file;
      }
    }
  }
  let best = null; let bestScore = 0;
  for (const file of files) {
    const base = path.basename(file, path.extname(file));
    const nbase = _norm(base);
    const segs = path.dirname(file).split(path.sep).map(_norm).filter(Boolean);
    let score = 0;
    for (const { q, w } of normQueries) {
      if (!q) continue;
      // Guard: avoid substring/fuzzy scoring for very short aliases like "nd".
      // Those should only match via the exact pass above (exact filename base or folder segment).
      if (q.length <= 2) {
        // Skip adding fuzzy/substring score for 1-2 char queries to prevent accidental hits like "soundcloud" -> "nd".
        continue;
      }
      if (segs.includes(q)) score += 6 * w; // folder name match is strong but below exact pass
  if (nbase === q) score += 5 * w;
  else if (nbase.length >= 3 && nbase.includes(q)) score += 3 * w; // require longer filename base
  else if (nbase.length >= 3 && q.length >= 3 && q.includes(nbase)) score += 2 * w; // require longer filename base
      else {
        // token overlap fallback
        const ov = tokenOverlapScore(nbase, q);
        if (ov >= 0.5) score += 1 * w;
      }
    }
    if (score > bestScore) { bestScore = score; best = file; }
  }
  if (!best) return null;
  // 'best' is already an absolute path from the recursive listing
  return best;
}

// Common alias map for projects/eras to file-name abbreviations
const COVER_ALIAS_MAP = new Map([
  ['goodbye & good riddance', ['gbgr','gbgr5ya']],
  ['goodbye and good riddance', ['gbgr','gbgr5ya']],
  ['death race for love', ['drfl']],
  ['wrld on drugs', ['wod']],
  ['legends never die', ['lnd']],
  ['fighting demons', ['fd']],
  ['the party never ends', ['tpne']],
  ['juiced up the ep', ['jute']],
  ['999', ['jw999','hh999']],
  ['up next', ['upnext']],
  ['too soon', ['toosoon']],
  ['pre party', ['preparty']],
  ['golden x getaway', ['golden x getaway']],
  ['playing games', ['playing games']],
  ['rockstar girl', ['rockstar girl']],
  ['bdm', ['bdm']],
  ['outsiders', ['out']],
  ['posthumous', ['post']],
  ['new dawn', ['nd']],
]);

function makeAliasesFor(value, opts = {}) {
  const { includeInitialism = true, includeHeuristics = true, minLen = 2 } = opts;
  const out = new Set();
  const base = _norm(value);
  if (!base) return [];
  if (base.length >= minLen) out.add(base);
  // Strip common decorations
  const stripped = base
    .replace(/\b(sessions?)\b/g, '')
    .replace(/\b(era|the|ep|album|mixtape)\b/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
  if (stripped && stripped !== base && stripped.length >= minLen) out.add(stripped);
  // initialism (first letters)
  if (includeInitialism) {
    const parts = stripped.split(' ').filter(Boolean);
    const initialism = parts.map(w => w[0]).join('');
    if (initialism.length >= minLen) out.add(initialism);
  }
  // alias map hits
  const aliasHits = COVER_ALIAS_MAP.get(base) || COVER_ALIAS_MAP.get(stripped);
  if (aliasHits) aliasHits.forEach(a => { const n = _norm(a); if (n.length >= minLen) out.add(n); });
  // heuristic synonyms
  if (includeHeuristics) {
    const t = stripped;
    if (/(goodbye|g\s*\&?\s*good\s*riddance)/.test(t) || (t.includes('goodbye') && t.includes('riddance'))) { out.add('gbgr'); out.add('gbgr5ya'); }
    if (t.includes('death') && t.includes('race') && t.includes('love')) { out.add('drfl'); }
    if (t.includes('wrld') && t.includes('drugs')) { out.add('wod'); }
    if (t.includes('legends') && t.includes('never') && t.includes('die')) { out.add('lnd'); }
    if (t.includes('fighting') && t.includes('demons')) { out.add('fd'); }
    if (t.includes('party') && t.includes('never') && t.includes('ends')) { out.add('tpne'); }
    if (t.includes('juiced') || t.includes('jute')) { out.add('jute'); }
    if (t.includes('outsiders')) { out.add('out'); }
    if (t.includes('posthumous')) { out.add('post'); }
    if (t.includes('new') && t.includes('dawn')) { out.add('nd'); }
    if (t.includes('999')) { out.add('jw999'); out.add('hh999'); }
  }
  return Array.from(out);
}

function findCoverForSong(song) {
  try {
    let queries = [];
    // Strict rule: released => match project; unreleased/unsurfaced => match era
    if (song.type === 'released') {
      if (song.project) queries.push(...makeAliasesFor(song.project).map(q => ({ q, w: 10 })));
    } else {
      if (song.era && String(song.era).trim()) {
        queries.push(...makeAliasesFor(song.era).map(q => ({ q, w: 10 })));
      } else if (song.project) {
        // Fallback only if no era present
        queries.push(...makeAliasesFor(song.project).map(q => ({ q, w: 6 })));
      }
    }
    // Manual overrides first
    const typeKey = (song.type === 'released') ? 'released' : 'unreleased';
    const keyRaw = (song.type === 'released') ? (song.project || '') : (song.era || song.project || '');
    const key = _norm(keyRaw);
    const override = COVER_MAP?.[typeKey]?.[key];
    if (override) {
      const overridePath = path.isAbsolute(override) ? override : path.join(__dirname, override);
      if (fs.existsSync(overridePath)) {
        try { console.log(`[covers] Using override for ${typeKey} "${keyRaw}": ${overridePath}`); } catch {}
        return overridePath;
      }
    }
    const dir = (song.type === 'released') ? COVER_DIR_RELEASED : COVER_DIR_UNRELEASED;
    const p = _bestImageByQueries(dir, queries);
    try { if (p) console.log(`[covers] Probes for ${song.type}:`, queries.map(x=>x.q || x).join(', ')); } catch {}
    if (p) return p;
    // General fallback: try song-name based aliases in the primary directory
    if (song.name) {
      const nameQueries = makeAliasesFor(song.name, { includeInitialism: false, includeHeuristics: false, minLen: 3 }).map(q => ({ q, w: 10 }));
      const byName = _bestImageByQueries(dir, nameQueries);
      if (byName) {
        try { console.log(`[covers] Name fallback for "${song.name}": ${byName}`); } catch {}
        return byName;
      }
    }
  // Released singles fallback: prefer Singles & Features subtree
    if (song.type === 'released') {
      const projNorm = _norm(song.project || '');
      const isSingles = !projNorm || /\bsingle|features?\b/.test(projNorm);
      if (isSingles && song.name) {
  const singleQueries = makeAliasesFor(song.name, { includeInitialism: false, includeHeuristics: false, minLen: 3 }).map(q => ({ q, w: 10 }));
        let singlesRoots = [];
        const preferredSingles = path.join(COVER_DIR_RELEASED, '20. Singles & Features');
        if (fs.existsSync(preferredSingles)) singlesRoots.push(preferredSingles);
        try {
          const entries = fs.readdirSync(COVER_DIR_RELEASED, { withFileTypes: true });
          for (const e of entries) {
            if (e.isDirectory() && /single|features?/i.test(e.name) && path.join(COVER_DIR_RELEASED, e.name) !== preferredSingles) {
              singlesRoots.push(path.join(COVER_DIR_RELEASED, e.name));
            }
          }
        } catch {}
        for (const root of singlesRoots) {
          const sp = _bestImageByQueries(root, singleQueries);
          if (sp) {
            try { console.log(`[covers] Singles match for "${song.name}": ${sp}`); } catch {}
            return sp;
          }
        }
        // Last resort: try entire released by song name
        const spAll = _bestImageByQueries(COVER_DIR_RELEASED, singleQueries);
        if (spAll) return spAll;
      }
    }
    // Fallback to generic covers folder
    const pg = _bestImageByQueries(COVER_DIR, queries);
    return pg || null;
  } catch { return null; }
}

function buildEmbedPayload(song) {
  const embed = buildSongEmbed(song);
  const coverPath = findCoverForSong(song);
  if (coverPath && fs.existsSync(coverPath)) {
    const fileName = 'cover' + path.extname(coverPath).toLowerCase();
    embed.setThumbnail('attachment://' + fileName);
    try { console.log(`[covers] Using thumbnail for "${song.name}": ${coverPath}`); } catch {}
    return { embeds: [embed], files: [{ attachment: coverPath, name: fileName }] };
  }
  if (coverPath && !fs.existsSync(coverPath)) {
    try { console.warn(`[covers] Chosen cover not found on disk: ${coverPath}`); } catch {}
  }
  return { embeds: [embed] };
}

// Try to resolve a sheet song entry by name to enrich metadata (type/project/era)
function findSheetSongByName(name) {
  if (!name) return null;
  const nq = normalizeStr(name);
  let best = null; let bestScore = 0;
  for (const s of Object.values(sheetSongLibrary)) {
  const hay = [s.name || '', (s.aliases || []).join(' '), s.project || '', s.era || '', s.version || ''].join(' ');
    const score = tokenOverlapScore(hay, name);
    if ((normalizeStr(s.name) === nq) || score > bestScore) {
      best = s; bestScore = score;
    }
  }
  return best;
}

if (!DISCORD_TOKEN) {
  console.warn('⚠️ No DISCORD_TOKEN found. Set it in environment or config.json to log in.');
} else {
  client.login(DISCORD_TOKEN).catch(err => {
    console.error('❌ Login error:', err);
    process.exit(1);
  });
}

// Pause/resume based on VC presence and auto-disconnect after timeout
client.on('voiceStateUpdate', (oldState, newState) => {
  // Keep track if the bot itself moved channels
  try {
    if (newState.id === client.user.id) {
      if (newState.channelId && newState.channelId !== connectedChannelId) {
        connectedChannelId = newState.channelId; pausedForEmpty = false; clearEmptyDisconnectTimer();
      } else if (!newState.channelId) {
        connectedChannelId = null; pausedForEmpty = false; clearEmptyDisconnectTimer();
      }
    }
  } catch {}
  const ch = getConnectedVoiceChannel();
  if (!ch) return;
  const affected = oldState.channelId === ch.id || newState.channelId === ch.id;
  if (!affected) return;
  if (!hasNonBotListeners()) {
    try { player.pause(); } catch {}
    pausedForEmpty = true; scheduleEmptyDisconnect();
  } else {
    clearEmptyDisconnectTimer();
    if (pausedForEmpty) {
      try { player.unpause(); } catch {}
      pausedForEmpty = false;
      if (!isPlaying && songQueue.length > 0) { playNextSong(); }
    }
  }
});

async function handleSongsBrowseCommand(message) {
  if (Object.keys(SONG_LIBRARY).length === 0) return message.reply('❌ No songs found! Add files to the /songs folder.');
  // group by artist
  const byArtist = new Map();
  const all = [];
  for (const [file, displayName] of Object.entries(SONG_LIBRARY)) {
    const meta = SONG_META[file] || { displayName, title: displayName, artist: 'Unknown Artist', path: path.join(config.MUSIC_FOLDER, file) };
    const artist = meta.artist || (displayName.includes(' - ') ? displayName.split(' - ')[0].trim() : 'Unknown Artist');
  const entry = { artist, title: meta.title || (displayName.includes(' - ') ? displayName.split(' - ').slice(1).join(' - ').trim() : displayName), displayName: meta.title ? `${artist} - ${meta.title}` : displayName, file, path: meta.path };
    if (!byArtist.has(artist)) byArtist.set(artist, []);
    byArtist.get(artist).push(entry);
    all.push(entry);
  }
  // build artist options
  const artists = Array.from(byArtist.keys()).sort((a,b) => a.localeCompare(b));
  const MAX = 25;
  const token = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  songsInteractions.set(token, { byArtist, all });
  const options = artists.slice(0, MAX).map(a => ({ label: a.slice(0, 100), value: a }));
  const select = new Discord.StringSelectMenuBuilder().setCustomId(`songs_artist:${token}`).setPlaceholder('Select an artist').addOptions(options);
  const row = new Discord.ActionRowBuilder().addComponents(select);
  return void message.reply({ content: 'Pick an artist:', components: [row] });
}

function buildArtistSongsEmbed(artist, songs, page = 1, pageSize = 25) {
  const totalPages = Math.max(1, Math.ceil(songs.length / pageSize));
  const p = Math.min(Math.max(1, page), totalPages);
  const start = (p - 1) * pageSize;
  const slice = songs.slice(start, start + pageSize);
  const lines = slice.map(s => `• ${s.title || s.displayName} (${s.file})`);
  const embed = new Discord.EmbedBuilder()
    .setTitle(`🎵 Songs by ${artist} [${p}/${totalPages}]`)
    .setDescription(`${lines.join('\n')}${songs.length > pageSize ? `\n\nUse ◀️ ▶️ to change pages.` : ''}\n\nUse !play <filename> to queue a song.`)
    .setColor('#7289DA');
  return { embed, page: p, totalPages };
}

function formatTimeMs(ms) {
  try {
    ms = Math.max(0, Math.floor(ms || 0));
    const totalSec = Math.floor(ms / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    const pad = (n) => String(n).padStart(2, '0');
    if (h > 0) return `${h}:${pad(m)}:${pad(s)}`;
    return `${m}:${pad(s)}`;
  } catch { return '0:00'; }
}

// ================== VC PRESENCE HELPERS ==================
function getConnectedVoiceChannel() {
  try {
    if (!connectedChannelId) return null;
    const ch = client.channels.cache.get(connectedChannelId);
    return ch && typeof ch.isVoiceBased === 'function' && ch.isVoiceBased() ? ch : null;
  } catch { return null; }
}
function hasNonBotListeners() {
  const ch = getConnectedVoiceChannel();
  if (!ch || !ch.members) return false;
  return ch.members.filter(m => !m.user.bot).size > 0;
}
function clearEmptyDisconnectTimer() {
  if (emptyVcTimeout) { clearTimeout(emptyVcTimeout); emptyVcTimeout = null; }
}
function scheduleEmptyDisconnect() {
  if (emptyVcTimeout) return;
  emptyVcTimeout = setTimeout(() => {
    try {
      if (!hasNonBotListeners()) {
        try { console.log('🔌 Disconnecting after empty VC timeout'); } catch {}
        if (connection) { try { player.stop(true); } catch {}; try { connection.destroy(); } catch {}; }
        connection = null; connectedChannelId = null; isPlaying = false; currentSong = null; pausedForEmpty = false; songQueue.length = 0;
        try { clearVoiceChannelStatus().catch(() => {}); } catch {}
      }
    } finally {
      clearEmptyDisconnectTimer();
    }
  }, EMPTY_VC_DISCONNECT_MS);
}

// ================== VOICE CHANNEL STATUS ==================
const STATUS_STATE_FILE = path.join(__dirname, 'status_state.json');
let STATUS_STATE = {};
try { if (fs.existsSync(STATUS_STATE_FILE)) STATUS_STATE = JSON.parse(fs.readFileSync(STATUS_STATE_FILE, 'utf8') || '{}'); } catch { STATUS_STATE = {}; }
function saveStatusState() { try { fs.writeFileSync(STATUS_STATE_FILE, JSON.stringify(STATUS_STATE, null, 2)); } catch {} }

function songTitleForPresence() {
  const t = currentSong?.title || '';
  return t ? (t.length > 120 ? t.slice(0, 117) + '...' : t) : '';
}
function setBotPresenceForSong() {
  try {
    const t = songTitleForPresence();
    if (t) client.user.setActivity(t, { type: Discord.ActivityType.Listening });
    else client.user.setActivity('!help', { type: Discord.ActivityType.Listening });
  } catch {}
}

async function setVoiceChannelStatus(text) {
  try {
    // Only update bot presence; do not alter channels or post messages
    setBotPresenceForSong();
  } catch {}
}

async function clearVoiceChannelStatus() {
  try { client.user.setActivity('!help', { type: Discord.ActivityType.Listening }); } catch {}
}

// ================== UZI GOOGLE SHEETS (basic) ==================
async function loadUziSongInfoFromSheets() {
  try {
    const sheetsClient = await getSheetsClient();
    if (!sheetsClient) { console.log('⚠️ Google Sheets not available for Uzi'); return; }
    if (!UZI_GOOGLE_SHEETS_CONFIG.SHEET_ID) { console.log('⚠️ UZI_SHEET_ID not configured'); return; }
    console.log('📊 Loading Uzi song info from Google Sheets...');
    uziSongLibrary = {};
    const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: UZI_GOOGLE_SHEETS_CONFIG.SHEET_ID });
  const titles = (meta.data.sheets || []).map(s => s.properties.title);
  // Scan the 2nd and 3rd tabs (index 1 and 2). If missing, fall back to whatever is available.
  let target = titles.slice(1, 3);
  if (target.length === 0) target = titles.slice(0, 2);
    if (target.length === 0) { console.warn('⚠️ No Uzi tabs found (need Released/Unreleased).'); return; }
  try { console.log('[Uzi] Target tabs:', target); } catch {}
    const ranges = target.map(t => `'${t.replace(/'/g, "''")}'!A1:AG20000`);
    const resp = await sheetsClient.spreadsheets.values.batchGet({ spreadsheetId: UZI_GOOGLE_SHEETS_CONFIG.SHEET_ID, ranges });
    let total = 0;
  const parseUziSheet = (values, sheetName) => {
      if (!values || values.length === 0) return 0;
      // Columns per screenshots: Era | Name | Notes | Track Length | Date of Recording/Release | Type | Portion/Streaming | Quality etc.
      // We'll heuristic-pick title/name, project/era, dates, type.
      const headerRow = values.findIndex(r => (r || []).some(c => /name|title/i.test(String(c || ''))));
      const startRow = headerRow >= 0 ? headerRow + 1 : 1;
      const headers = (values[headerRow] || []).map(x => (x || '').toString().trim().toLowerCase());
      const colIdx = (keys) => headers.findIndex(h => keys.some(k => h.includes(k)));
      const idxEra = colIdx(['era']);
      const idxName = colIdx(['name','title','track']);
      const idxNotes = colIdx(['notes']);
      const idxRecDate = colIdx(['date of recording','record date','recording date']);
      const idxRelDate = colIdx(['release date']);
  const idxType = colIdx(['type']);
  const idxPortion = colIdx(['portion']);
  const idxQuality = colIdx(['quality']);
  const idxStreaming = colIdx(['streaming','stream']);
      const idxLength = colIdx(['length','track length','duration']);
      let count = 0;
      for (let r = startRow; r < values.length; r++) {
        const row = values[r] || [];
        const cell = (i) => (i >= 0 && row[i] != null ? String(row[i]).trim() : '');
        const name = cell(idxName);
        if (!name) continue;
        const era = cell(idxEra);
        const notes = cell(idxNotes);
        const recordDate = cell(idxRecDate);
        const releaseDate = cell(idxRelDate);
        const length = cell(idxLength);
        const releaseState = (/unreleased|throwaway|demo|snippet|leak/i.test(sheetName) || /snippet|throwaway|demo|unreleased/i.test(cell(idxType))) ? 'unreleased' : 'released';
        const portion = cell(idxPortion);
        const quality = cell(idxQuality);
        const streaming = cell(idxStreaming);
        const typeLabel = cell(idxType);
        const key = `${name.toLowerCase()}|${(era||'').toLowerCase()}|${releaseState}|${sheetName.toLowerCase()}|${count}`;
        uziSongLibrary[key] = {
          artist: 'Lil Uzi Vert', name, era, notes,
          recordDate, releaseDate, duration: length,
          type: releaseState, category: sheetName, sheetSource: 'uzi',
          portion, quality, streaming, variantType: typeLabel
        };
        count++;
      }
      return count;
    };
    const valueRanges = resp.data.valueRanges || [];
    for (let i = 0; i < valueRanges.length; i++) {
      const vr = valueRanges[i];
      const m = /'([^']+)'!/i.exec(vr.range || '');
      const sheetName = (m && m[1]) || target[i] || 'Unknown';
      const added = parseUziSheet(vr.values, sheetName);
      console.log(`✅ Uzi: Found ${added} entries in "${sheetName}"`);
      total += added;
    }
    console.log(`✅ Uzi: Total entries loaded: ${total}`);
  } catch (e) {
    console.error('❌ Error loading Uzi sheets:', e?.message || e);
  }
}

function buildUziEmbed(song) {
  const embed = new Discord.EmbedBuilder()
    .setTitle(`🎵 ${song.name}`)
    .setColor(song.type === 'released' ? '#1DB954' : '#FF6B35')
    .addFields(
      ...(song.era ? [{ name: '🕰️ Era', value: String(song.era).slice(0,1024), inline: true }] : []),
      ...(song.releaseDate ? [{ name: '🚀 Release Date', value: String(song.releaseDate).slice(0,1024), inline: true }] : []),
      ...(song.recordDate ? [{ name: '🗓️ Record Date', value: String(song.recordDate).slice(0,1024), inline: true }] : []),
      ...(song.duration ? [{ name: '⌱ Duration', value: String(song.duration).slice(0,1024), inline: true }] : []),
      ...(song.category ? [{ name: '📂 Tab', value: String(song.category).slice(0,1024), inline: true }] : [])
    );
  if (song.type === 'unreleased') {
    if (song.portion) embed.addFields({ name: '🧩 Portion', value: String(song.portion).slice(0,1024), inline: true });
    if (song.quality) embed.addFields({ name: '🎚️ Quality', value: String(song.quality).slice(0,1024), inline: true });
  } else if (song.type === 'released') {
    if (song.variantType) embed.addFields({ name: '🏷️ Type', value: String(song.variantType).slice(0,1024), inline: true });
    if (song.streaming) embed.addFields({ name: '📡 Streaming', value: String(song.streaming).slice(0,1024), inline: true });
  }
  if (song.notes) embed.addFields({ name: 'ℹ️ Notes', value: String(song.notes).slice(0,1024), inline: false });
  return { embeds: [embed] };
}

async function handleUziInfoQuery(message, rawQuery) {
  const q = (rawQuery || '').trim();
  if (!UZI_GOOGLE_SHEETS_CONFIG.SHEET_ID) return void message.reply('❌ UZI_SHEET_ID not configured. Ask the owner to set it in .env or config.json.');
  if (!q) return void message.reply('❌ Please specify a song name. Example: `!uziinfo 20 min`');
  const normQuery = normalizeStr(q);
  const candidates = [];
  for (const [key, song] of Object.entries(uziSongLibrary)) {
    const hay = [key, song.name || '', song.era || '', song.notes || '', song.category || ''].join(' ');
    const normHay = normalizeStr(hay);
    if (normHay.includes(normQuery)) candidates.push(song);
  }
  let matches = candidates;
  if (matches.length === 0) {
    const scored = [];
    for (const [key, song] of Object.entries(uziSongLibrary)) {
      const hay = [key, song.name || '', song.era || '', song.notes || '', song.category || ''].join(' ');
      const score = tokenOverlapScore(hay, q);
      if (score > 0) scored.push({ song, score });
    }
    scored.sort((a,b) => b.score - a.score);
    matches = scored.slice(0, 50).map(s => s.song);
  }
  if (matches.length === 0) return void message.reply(`❌ No Uzi songs found matching "${q}".`);

  const MAX_OPTIONS = 25;
  const token = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const limited = matches.slice(0, MAX_OPTIONS);
  uziInfoInteractions.set(token, limited);
  const options = limited.map((s, idx) => {
    const label = `${s.name}`.slice(0, 100);
    const info = s.type === 'released' ? (s.releaseDate || 'Released') : (s.era || 'Unreleased');
    const desc = `${info} • ${s.category || ''}`.slice(0, 100);
    return { label, description: desc, value: String(idx) };
  });
  const select = new Discord.StringSelectMenuBuilder().setCustomId(`uziinfo_select:${token}`).setPlaceholder('Select a song/version').addOptions(options);
  const row = new Discord.ActionRowBuilder().addComponents(select);
  const preview = limited.map((s, i) => {
    const emoji = s.type === 'released' ? '🟢' : '🟡';
    const info = s.type === 'released' ? (s.releaseDate || 'Released') : (s.era || 'Unreleased');
    return `**${i + 1}. ${s.name}**\n${emoji} ${info} • ${s.category || ''}`;
  }).join('\n\n');
  const embed = new Discord.EmbedBuilder().setTitle(`🎵 Select a track: ${q}`).setDescription(preview).setColor('#9B59B6');
  return void message.reply({ embeds: [embed], components: [row] });
}

// ===== Autoplay helpers =====
function shuffleArray(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

async function buildAutoShuffleDeck() {
  try {
    // Reuse SONG_LIBRARY; if empty, rebuild
    if (!SONG_LIBRARY || Object.keys(SONG_LIBRARY).length === 0) {
      SONG_LIBRARY = await buildSongLibrary();
    }
    const entries = Object.keys(SONG_LIBRARY).filter(f => {
      const ext = path.extname(f).toLowerCase();
      return config.SUPPORTED_FORMATS.includes(ext);
    });
    autoShuffleDeck = shuffleArray(entries);
    autoDeckIndex = 0;
  } catch (e) {
    console.error('autoplay deck build error:', e);
    autoShuffleDeck = []; autoDeckIndex = 0;
  }
}

async function enqueueNextAutoTrack() {
  try {
    if (!autoShuffleDeck || autoDeckIndex >= autoShuffleDeck.length) {
      await buildAutoShuffleDeck();
    }
    const file = autoShuffleDeck[autoDeckIndex++];
    if (!file) return false;
    const filePath = path.join(config.MUSIC_FOLDER, file);
    const title = SONG_LIBRARY[file] || file;
  songQueue.push({ type: 'file', path: filePath, title, auto: true, source: 'local' });
    return true;
  } catch (e) {
    console.error('enqueueNextAutoTrack error:', e);
    return false;
  }
}

async function buildMegaShuffleDeck() {
  try {
    if (!config.MEGA_ENABLED || (!MegaFile && !MegaStorage)) { megaShuffleDeck = []; megaDeckIndex = 0; return; }
    await ensureMegaReadyFromEnv();
    if (megaFilesIndex.size === 0) { megaShuffleDeck = []; megaDeckIndex = 0; return; }
    const entries = Array.from(megaFilesIndex.entries())
      .filter(([name, entry]) => {
        const n = String(name || '');
        return isSupportedAudioName(n);
      })
      .map(([name, entry]) => ({ name, entry }));
    // Shuffle and store
    const shuffled = shuffleArray(entries.slice());
    megaShuffleDeck = shuffled;
    megaDeckIndex = 0;
  } catch (e) {
    console.error('mega autoplay deck build error:', e);
    megaShuffleDeck = []; megaDeckIndex = 0;
  }
}

async function enqueueNextMegaAutoTrack() {
  try {
    if (!megaShuffleDeck || megaDeckIndex >= megaShuffleDeck.length) {
      await buildMegaShuffleDeck();
    }
    const item = megaShuffleDeck[megaDeckIndex++];
    if (!item) return false;
    const { entry } = item;
    const file = entry?.file || (entry?.link && MegaFile ? MegaFile.fromURL(entry.link) : null);
    if (!file) return false;
    let stream = null;
    try { if (typeof file.download === 'function') stream = file.download({ start: 0 }); } catch {}
    if (!stream) { try { if (typeof file.createReadStream === 'function') stream = file.createReadStream({ initialChunkSize: 1024*1024, chunkSize: 1024*1024 }); } catch {} }
    if (!stream) return false;
    const resource = createFfmpegResourceFromReadable(stream);
    const title = file.name || 'MEGA Auto';
    songQueue.push({ resource, title, auto: true, source: 'external' });
    return true;
  } catch (e) {
    console.error('enqueueNextMegaAutoTrack error:', e);
    return false;
  }
}

async function handleAutoPlayCommand(message, modeWord) {
  const word = (modeWord || '').trim().toLowerCase();
  if (!word) {
    return void message.reply(`🔁 Autoplay is currently ${autoPlayEnabled ? 'ON' : 'OFF'} (mode: ${autoPlayMode}). Use !autoplay on|off or !megashuffle on|off`);
  }
  const turnOn = word === 'on' || word === 'enable' || word === 'enabled' || word === 'true' || word === '1';
  const turnOff = word === 'off' || word === 'disable' || word === 'disabled' || word === 'false' || word === '0';
  if (!turnOn && !turnOff) return void message.reply('❌ Usage: !autoplay on|off');
  autoPlayEnabled = turnOn;
  if (autoPlayEnabled) {
  autoPlayMode = 'local';
  await buildAutoShuffleDeck();
    message.reply('🔁 Autoplay enabled. I will shuffle local songs when the queue is empty.');
    // If idle with nothing playing, kick it off
    if (!isPlaying && songQueue.length === 0 && connection && hasNonBotListeners()) {
  playNextSong();
    }
  } else {
    message.reply('⏹️ Autoplay disabled.');
  }
}

async function handleMegaShuffleCommand(message, modeWord) {
  const word = (modeWord || '').trim().toLowerCase();
  if (!config.MEGA_ENABLED) return void message.reply('❌ MEGA is not enabled/configured. Set MEGA_FOLDER_LINK (and key) or login via !megalogin.');
  if (!word) {
    return void message.reply(`☁️ MEGA shuffle is ${autoPlayEnabled && autoPlayMode==='mega' ? 'ON' : 'OFF'}. Use !megashuffle on|off`);
  }
  const turnOn = word === 'on' || word === 'enable' || word === 'enabled' || word === 'true' || word === '1';
  const turnOff = word === 'off' || word === 'disable' || word === 'disabled' || word === 'false' || word === '0';
  if (!turnOn && !turnOff) return void message.reply('❌ Usage: !megashuffle on|off');
  if (turnOn) {
    autoPlayMode = 'mega';
    autoPlayEnabled = true;
    await buildMegaShuffleDeck();
    message.reply('☁️🔁 MEGA shuffle enabled. I will shuffle MEGA files when the queue is empty.');
    if (!isPlaying && songQueue.length === 0 && connection && hasNonBotListeners()) {
      playNextSong();
    }
  } else {
    if (autoPlayMode === 'mega') {
      autoPlayEnabled = false;
      message.reply('⏹️ MEGA shuffle disabled.');
    } else {
      message.reply('ℹ️ MEGA shuffle was not active.');
    }
  }
}

// ================== EXTERNAL STREAM PROVIDER (licensed API hook) ==================
// Contract:
// input: { query: string, artistHint?: string }
// output: { ok: true, url: string, title?: string } | { ok: false, error?: string }
async function resolveExternalStream({ query, artistHint }) {
  try {
    if (!config.EXTERNAL_STREAM_ENABLED) return { ok: false, error: 'disabled' };
    if (!config.EXTERNAL_STREAM_API_URL) return { ok: false, error: 'no-endpoint' };
    const payload = { q: String(query || '').trim(), artist: String(artistHint || '') };
    const res = await fetch(config.EXTERNAL_STREAM_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(config.EXTERNAL_STREAM_API_KEY ? { 'Authorization': `Bearer ${config.EXTERNAL_STREAM_API_KEY}` } : {})
      },
      body: JSON.stringify(payload)
    }).catch(() => null);
    if (!res || !res.ok) return { ok: false, error: 'unreachable' };
    const j = await res.json().catch(() => ({}));
    // Expect { streamUrl: string, title?: string }
    const streamUrl = j?.streamUrl || j?.url;
    if (!streamUrl || typeof streamUrl !== 'string') return { ok: false, error: 'bad-response' };
    // Basic safety: must be http(s) URL
    if (!/^https?:\/\//i.test(streamUrl)) return { ok: false, error: 'invalid-url' };
    return { ok: true, url: streamUrl, title: j?.title };
  } catch (e) {
    try { console.warn('External provider error:', e?.message || e); } catch {}
    return { ok: false, error: 'exception' };
  }
}

function isUrl(s) { try { const u = new URL(String(s)); return u.protocol === 'http:' || u.protocol === 'https:'; } catch { return false; } }
function isYouTubeUrl(s) { return /(^https?:\/\/(?:www\.)?youtu(?:\.be|be\.com)\/)/i.test(String(s || '')); }
function stripTrailingSlash(u) {
  const s = String(u || '');
  // If ends with / optionally before a query, drop the slash
  return s.replace(/\/(?=\?|$)/, '');
}

// ================== JUICE WRLD API player resolver ==================
// Attempts to find a playable song via https://juicewrldapi.com/player/songs/
// Returns: { ok: true, url, title } or { ok: false }
async function resolveJuiceWrldPlayer(query) {
  try {
    if (!config.JUICEWRLD_PLAYER_ENABLED) return { ok: false, error: 'disabled' };
    const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
  const debug = { tried: [] };
    // Try search endpoint first, then plain list; read a few pages to find a match
    const tryFetch = async (u) => {
      debug.tried.push(u);
      const r = await fetch(u, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'Referer': base } }).catch(() => null);
      if (!r || !r.ok) return null;
      try { return await r.json(); } catch { return null; }
    };
    // 0) Prefer direct JSON search on /songs to get IDs quickly
    let results = [];
    const songsBase = (config.JUICEWRLD_SONGS_ENDPOINT && config.JUICEWRLD_SONGS_ENDPOINT.trim()) || `${base}/songs/`;
    const songsSearchUrls = [
      `${songsBase}?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}`,
      `${songsBase}?page=1&page_size=50&search=${encodeURIComponent(query)}`,
      `${stripTrailingSlash(songsBase)}.json?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}`,
      `${stripTrailingSlash(songsBase)}.json?page=1&page_size=50&search=${encodeURIComponent(query)}`,
      `${base}/api/songs/?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}`,
      `${base}/api/songs/?page=1&page_size=50&search=${encodeURIComponent(query)}`,
      `${base}/api/songs.json?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}`,
      `${base}/api/songs.json?page=1&page_size=50&search=${encodeURIComponent(query)}`
    ];
    for (const su of songsSearchUrls) {
      const sj = await tryFetch(su);
      if (sj && typeof sj === 'object') {
        const arr = Array.isArray(sj.results) ? sj.results : (Array.isArray(sj) ? sj : []);
        if (arr.length) { results.push(...arr); debug.method = 'songs-json'; debug.endpoint = su; break; }
      }
    }
    // If still no results from /songs, try scraping the HTML search pages
    if (results.length === 0) {
      for (const su of songsSearchUrls) {
        debug.tried.push(su);
        const r = await fetch(su, { headers: { 'User-Agent': 'Mozilla/5.0' } }).catch(() => null);
        if (!r || !r.ok) continue;
        const ctype = String(r.headers.get('content-type') || '');
        if (!/text\/html/i.test(ctype)) continue;
        const html = await r.text().catch(() => '');
        if (!html) continue;
        const embedded = extractEmbeddedJsonFromHtml(html);
        if (embedded) {
          const arr = findFirstArrayOfObjectsWithAnyKey(embedded, ['id','pk','title','name','track_title','song_title']);
          if (Array.isArray(arr) && arr.length) { results.push(...arr); debug.method = 'songs-embedded-json'; debug.endpoint = su; break; }
        }
        // anchor fallback for song detail/play links
        const anchorRe = /<a[^>]+href=["']([^"']*\/player\/songs\/(\d+)(?:\/play\/)?)["'][^>]*>([\s\S]*?)<\/a>/gi;
        let m; let bestA = null; let bestScoreA = 0;
        const normT = (s) => (s || '').toString().replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g,' ').replace(/\s+/g,' ').trim();
        while ((m = anchorRe.exec(html)) !== null) {
          const id = m[2]; const text = normT(m[3]);
          if (!id) continue;
          const score = tokenOverlapScore(text, query);
          if (!bestA || score > bestScoreA) { bestA = { id, text, href: m[1] }; bestScoreA = score; }
        }
        if (bestA) { results.push({ id: bestA.id, title: bestA.text, name: bestA.text }); debug.method = 'songs-html-anchors'; debug.endpoint = su; break; }
      }
    }
    // 0a) If still empty, try the JSON index /juicewrld (contains songs list with IDs)
    if (results.length === 0) {
      const indexJson = await tryFetch(`${base}/juicewrld`);
      if (indexJson && typeof indexJson === 'object') {
        let idxSongs = Array.isArray(indexJson.songs) ? indexJson.songs : null;
        if (!idxSongs && indexJson.songs && typeof indexJson.songs === 'object') {
          // Collect all arrays inside songs
          for (const k of Object.keys(indexJson.songs)) {
            const v = indexJson.songs[k];
            if (Array.isArray(v) && v.length) {
              idxSongs = v; break;
            }
          }
        }
        if (!idxSongs) {
          const arr = findFirstArrayOfObjectsWithAnyKey(indexJson, ['id','pk','title','name','track_title','song_title']);
          if (Array.isArray(arr)) idxSongs = arr;
        }
        if (Array.isArray(idxSongs) && idxSongs.length) {
          debug.method = 'index-json';
          debug.endpoint = `${base}/juicewrld`;
          // Use these as results to allow common matching logic
          results.push(...idxSongs);
        }
      }
    }
    const playerBase = (config.JUICEWRLD_PLAYER_SONGS_ENDPOINT && config.JUICEWRLD_PLAYER_SONGS_ENDPOINT.trim()) || `${base}/player/songs/`;
  const endpoints = [
      // Standard endpoints
      `${playerBase}?search=${encodeURIComponent(query)}`,
      `${playerBase}?format=json&search=${encodeURIComponent(query)}`,
      `${playerBase}`,
      `${playerBase}?format=json`,
      // .json variants
  `${stripTrailingSlash(playerBase)}.json`,
  `${stripTrailingSlash(playerBase)}.json?search=${encodeURIComponent(query)}`,
      // API-prefixed variants
      `${base}/api/player/songs/?search=${encodeURIComponent(query)}`,
      `${base}/api/player/songs/?format=json&search=${encodeURIComponent(query)}`,
      `${base}/api/player/songs/`,
      `${base}/api/player/songs/?format=json`,
      `${base}/api/player/songs.json`,
      `${base}/api/player/songs.json?search=${encodeURIComponent(query)}`
    ];
    // Include a few explicit pages (1..3)
    for (let p = 1; p <= 3; p++) {
      endpoints.push(
  `${playerBase}?page=${p}`,
  `${playerBase}?format=json&page=${p}`,
  `${stripTrailingSlash(playerBase)}.json?page=${p}`,
        `${base}/api/player/songs/?page=${p}`,
        `${base}/api/player/songs/?format=json&page=${p}`,
        `${base}/api/player/songs.json?page=${p}`
      );
    }
  // If we already got some results from /songs or /juicewrld, skip hitting player JSON endpoints until after matching
  if (results.length === 0) results = [];
  for (const ep of endpoints) {
      const data = await tryFetch(ep);
      if (!data) continue;
      const pageItems = Array.isArray(data) ? data : (Array.isArray(data.results) ? data.results : []);
      results.push(...pageItems);
      // if paginated and no search, grab a couple more pages to widen match space
      if (!Array.isArray(data) && data.next && results.length < 100) {
        const next1 = await tryFetch(data.next);
        if (next1) {
          const more = Array.isArray(next1) ? next1 : (Array.isArray(next1.results) ? next1.results : []);
          results.push(...more);
        }
      }
      if (results.length > 0) { debug.method = 'json'; debug.endpoint = ep; break; }
    }
    // If still no JSON, attempt HTML scrape for embedded JSON
  if (results.length === 0) {
      const htmlEndpoints = [];
      // Try paginated player pages first
      for (let p = 1; p <= 3; p++) {
        htmlEndpoints.push(
    `${playerBase}?page=${p}`,
    `${playerBase}?page=${p}&search=${encodeURIComponent(query)}`
        );
      }
      // Fallbacks without explicit page
      htmlEndpoints.push(
  `${playerBase}`,
  `${playerBase}?search=${encodeURIComponent(query)}`,
  `${songsBase}?search=${encodeURIComponent(query)}`,
  `${songsBase}`
      );
      for (const ep of htmlEndpoints) {
        debug.tried.push(ep);
        const r = await fetch(ep, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' } }).catch(() => null);
        if (!r || !r.ok) continue;
        const ctype = String(r.headers.get('content-type') || '');
        if (!/text\/html/i.test(ctype)) continue;
        const html = await r.text().catch(() => '');
        if (!html) continue;
        const embedded = extractEmbeddedJsonFromHtml(html);
        if (embedded) {
          const arr = findFirstArrayOfObjectsWithAnyKey(embedded, ['file_url','fileUrl','stream_url','streamUrl','audio_url','audioUrl','url']);
          if (Array.isArray(arr) && arr.length) { results.push(...arr); debug.method = 'embedded-json'; debug.endpoint = ep; break; }
        }
        // Anchor-based fallback: look for /player/songs/{id}/(play/)? links and use link text as title
        if (results.length === 0) {
          const anchorRe = /<a[^>]+href=["']([^"']*\/player\/songs\/(\d+)(?:\/play\/)?)["'][^>]*>([\s\S]*?)<\/a>/gi;
          let m; let bestA = null; let bestScoreA = 0;
          const norm = (s) => (s || '').toString().replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g,' ').replace(/\s+/g,' ').trim();
          while ((m = anchorRe.exec(html)) !== null) {
            const id = m[2]; const text = norm(m[3]);
            if (!id) continue;
            const score = tokenOverlapScore(text, query);
            if (!bestA || score > bestScoreA) { bestA = { id, text, href: m[1] }; bestScoreA = score; }
          }
          if (bestA) {
          results = [];
            results.push({ id: bestA.id, title: bestA.text, name: bestA.text, file_url: `/player/songs/${bestA.id}/play/` });
            debug.method = 'html-anchors'; debug.endpoint = ep; debug.anchorId = bestA.id; debug.anchorText = bestA.text;
            break;
          }
        }
      }
    }
  if (results.length === 0) return { ok: false, error: 'empty', debug };
    // Heuristic match by name/title fields
    const norm = (s) => (s || '').toString().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    const qn = norm(query);
    let best = null; let bestScore = 0;
    for (const r of results) {
      const name = r?.name || r?.title || r?.track_title || r?.song_title || '';
      const hay = [name, r?.original_key, (r?.track_titles || []).join(' ')].join(' ');
      const score = tokenOverlapScore(hay, query);
      if (norm(name) === qn || score > bestScore) { best = r; bestScore = score; }
    }
    if (!best) return { ok: false, error: 'no-match' };
    // Determine stream URL — prefer official play endpoint if id exists; else file_url (often relative), then common names
    let streamUrl = '';
    const idVal = best?.id ?? best?.pk ?? null;
    if (idVal != null && String(idVal).trim() !== '') {
      streamUrl = `${base}/player/songs/${encodeURIComponent(String(idVal))}/play/`;
      debug.id = String(idVal);
      debug.used = 'play-endpoint';
    }
    let chosenField = '';
    const rawUrl = streamUrl
      || (best.file_url ? (chosenField='file_url', best.file_url) : '')
      || (best.fileUrl ? (chosenField='fileUrl', best.fileUrl) : '')
      || (best.stream_url ? (chosenField='stream_url', best.stream_url) : '')
      || (best.streamUrl ? (chosenField='streamUrl', best.streamUrl) : '')
      || (best.audio_url ? (chosenField='audio_url', best.audio_url) : '')
      || (best.audioUrl ? (chosenField='audioUrl', best.audioUrl) : '')
      || (best.url ? (chosenField='url', best.url) : '')
      || (best.audio ? (chosenField='audio', best.audio) : '')
      || (best.playback ? (chosenField='playback', best.playback) : '');
    if (rawUrl && typeof rawUrl === 'string') {
      if (/^https?:\/\//i.test(rawUrl)) {
        streamUrl = rawUrl;
      } else {
        try { streamUrl = new URL(rawUrl, base).toString(); } catch { streamUrl = ''; }
      }
    }
  if (!streamUrl || !/^https?:\/\//i.test(streamUrl)) return { ok: false, error: 'no-url', debug };
    // Try to resolve redirect to a direct file URL for better ffmpeg compatibility
    try {
      const needsResolve = /\/player\/songs\/\d+\/play\/?$/i.test(streamUrl) || !/\.(mp3|m4a|aac|wav|ogg|opus)(\?|$)/i.test(streamUrl);
      if (needsResolve) {
        let res = await fetch(streamUrl, { method: 'HEAD', redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', 'Referer': config.JUICEWRLD_API_BASE || 'https://juicewrldapi.com' } }).catch(() => null);
        if (!res || !res.ok) {
          res = await fetch(streamUrl, { method: 'GET', redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', 'Range': 'bytes=0-0', 'Referer': config.JUICEWRLD_API_BASE || 'https://juicewrldapi.com' } }).catch(() => null);
        }
        const finalUrl = res && typeof res.url === 'string' ? res.url : '';
        if (finalUrl && /^https?:\/\//i.test(finalUrl)) {
          streamUrl = finalUrl;
          debug.finalUrl = finalUrl;
          debug.used = debug.used || 'play-redirect';
        }
        try { res?.body?.cancel?.(); } catch {}
      }
    } catch {}
    const title = best.title || best.name || best.track_title || best.song_title || query;
    if (!debug.used) debug.used = chosenField || 'unknown';
    return { ok: true, url: streamUrl, title, debug };
  } catch (e) {
    try { console.warn('JuiceWrld player resolve error:', e?.message || e); } catch {}
    return { ok: false, error: 'exception' };
  }
}

function _truncateJson(obj, max = 1200) {
  try { const s = JSON.stringify(obj, null, 2); return s.length > max ? s.slice(0, max - 3) + '...' : s; } catch { return String(obj).slice(0, max); }
}

// Try to extract JSON embedded in HTML (Next.js/Nuxt/inline JSON)
function extractEmbeddedJsonFromHtml(html) {
  try {
    const candidates = [
      /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i,
      /window\.__NEXT_DATA__\s*=\s*({[\s\S]*?});/i,
      /<script[^>]*type=["']application\/json["'][^>]*>([\s\S]*?)<\/script>/i,
      /window\.__NUXT__\s*=\s*({[\s\S]*?});/i
    ];
    for (const re of candidates) {
      const m = html.match(re);
      if (m && m[1]) {
        try { return JSON.parse(m[1]); } catch { /* ignore and continue */ }
      }
    }
  } catch {}
  return null;
}

// Find the first array of objects that contains any of the given keys in at least one element
function findFirstArrayOfObjectsWithAnyKey(root, keyCandidates, maxDepth = 7) {
  const seen = new Set();
  function helper(node, depth) {
    if (!node || depth > maxDepth) return null;
    if (typeof node !== 'object') return null;
    if (seen.has(node)) return null; seen.add(node);
    if (Array.isArray(node)) {
      const hasObj = node.find(v => v && typeof v === 'object' && !Array.isArray(v));
      if (hasObj) {
        const anyKey = node.find(v => v && typeof v === 'object' && keyCandidates.some(k => Object.prototype.hasOwnProperty.call(v, k)));
        if (anyKey) return node;
      }
      for (const v of node) { const r = helper(v, depth + 1); if (r) return r; }
      return null;
    }
    for (const k of Object.keys(node)) {
      const r = helper(node[k], depth + 1); if (r) return r;
    }
    return null;
  }
  return helper(root, 0);
}

async function handleJwTestCommand(message, query) {
  try {
    const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
    const urls = [
      `${base}/juicewrld`,
      `${base}/songs/`,
      `${base}/songs/?format=json`,
  `${base}/songs/?page=1&page_size=50&category=released`,
  `${base}/songs.json?page=1&page_size=50&category=released`,
  `${base}/api/songs/?page=1&page_size=50&category=released`,
  `${base}/api/songs.json?page=1&page_size=50&category=released`,
      `${base}/player/songs/`,
      `${base}/player/songs/?format=json`,
  `${base}/player/songs/?page=1`,
  `${base}/player/songs/?page=2`,
  `${base}/player/songs/?page=3`,
      `${base}/api/player/songs/`,
      `${base}/api/player/songs/?format=json`,
  query ? `${base}/songs/?search=${encodeURIComponent(query)}` : '',
  query ? `${base}/songs/?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/songs/?page=1&page_size=50&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/songs.json?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/songs.json?page=1&page_size=50&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/api/songs/?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/api/songs/?page=1&page_size=50&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/api/songs.json?page=1&page_size=50&category=released&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/api/songs.json?page=1&page_size=50&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/songs/?format=json&search=${encodeURIComponent(query)}` : '',
      query ? `${base}/player/songs/?search=${encodeURIComponent(query)}` : '',
      query ? `${base}/player/songs/?format=json&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/player/songs/?page=1&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/player/songs/?page=2&search=${encodeURIComponent(query)}` : '',
  query ? `${base}/player/songs/?page=3&search=${encodeURIComponent(query)}` : '',
      query ? `${base}/api/player/songs/?search=${encodeURIComponent(query)}` : '',
      query ? `${base}/api/player/songs/?format=json&search=${encodeURIComponent(query)}` : ''
    ].filter(Boolean);
    const results = [];
    for (const u of urls) {
      let status = 'ERR'; let count = null; let sample = null; let ctype = ''; let parsed = false;
      try {
  const res = await fetch(u, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' } });
        status = `${res.status}`;
        ctype = String(res.headers.get('content-type') || '');
        let body = null; try { body = await res.json(); parsed = true; } catch { body = null; }
        if (body && typeof body === 'object') {
          const arr = Array.isArray(body) ? body : (Array.isArray(body.results) ? body.results : null);
          if (arr) {
            count = body.count != null ? body.count : arr.length;
            sample = arr[0] || null;
          } else {
            sample = body;
          }
        }
      } catch (e) { status = 'ERR'; sample = { error: String(e?.message || e) }; }
      results.push({ url: u, status, count, ctype, parsed, keys: sample ? Object.keys(sample).slice(0, 12) : [], sample });
    }
    const lines = results.map(r => `• ${r.url}\n  status: ${r.status}${r.count!=null?`, count: ${r.count}`:''}\n  type: ${r.ctype || 'n/a'} | json: ${r.parsed ? 'yes' : 'no'}\n  keys: ${r.keys.join(', ')}`);
    const head = `JW API diagnostics${query?` (search: ${query})`:''} — base: ${base}`;
    const allText = [head, ...lines].join('\n');
    // If content is too long for a single Discord message, chunk it and also attach a full text file
    const MAX = 1900; // leave headroom
    if (allText.length <= MAX) {
      await message.reply({ content: allText });
    } else {
      const chunks = [];
      let buf = '';
      for (const ln of [head, ...lines]) {
        const add = (buf ? '\n' : '') + ln;
        if ((buf + add).length > MAX) { chunks.push(buf); buf = ln; }
        else { buf += add; }
      }
      if (buf) chunks.push(buf);
      for (let i = 0; i < chunks.length; i++) {
        const prefix = chunks.length > 1 ? `(${i+1}/${chunks.length}) ` : '';
        await message.reply({ content: prefix + chunks[i] });
      }
      // Attach full output as a file for convenience
      try {
        const txtPath = path.join(__dirname, 'jwtest-output.txt');
        fs.writeFileSync(txtPath, allText, 'utf8');
        await message.channel.send({ content: 'Full jwtest output attached:', files: [txtPath] });
        try { fs.unlinkSync(txtPath); } catch {}
      } catch {}
    }
    // Attach a trimmed JSON of the first sample for inspection
    const first = results.find(r => r.sample) || null;
    if (first) {
      const tmpPath = path.join(__dirname, 'jwtest-sample.json');
      fs.writeFileSync(tmpPath, JSON.stringify(first.sample, null, 2));
      try { await message.channel.send({ content: 'Sample JSON from first endpoint:', files: [tmpPath] }); }
      finally { try { fs.unlinkSync(tmpPath); } catch {} }
    }
    // Also show resolver debug and probe the resolved play URL headers
  if (query) {
      try {
        const probe = await resolveJuiceWrldPlayer(query);
        if (probe && probe.ok) {
          const u = probe.url;
          let status = 'ERR'; let ctype = ''; let via = 'HEAD';
          try {
            const r = await fetch(u, { method: 'HEAD', headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', 'Referer': config.JUICEWRLD_API_BASE || 'https://juicewrldapi.com' } });
            status = String(r.status);
            ctype = String(r.headers.get('content-type') || '');
          } catch {
            via = 'GET';
            // Fallback GET just for headers; do not consume body
            const r = await fetch(u, { method: 'GET', headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*', 'Referer': config.JUICEWRLD_API_BASE || 'https://juicewrldapi.com' } }).catch(() => null);
            if (r) { status = String(r.status); ctype = String(r.headers.get('content-type') || ''); try { r.body?.cancel?.(); } catch {} }
          }
          const dbg = probe.debug ? ` used=${probe.debug.used || ''} method=${probe.debug.method || ''} id=${probe.debug.id || ''}` : '';
          await message.channel.send({ content: `Resolver: ${probe.title}\nURL: ${u}\nHeaders (${via}): status=${status}, type=${ctype}${dbg}`.slice(0,1900) });
        } else {
          const dbg = probe && probe.debug ? `\nmethod=${probe.debug.method||''}\nendpoint=${probe.debug.endpoint||''}\ntried=${(probe.debug.tried||[]).slice(-6).join('\n')}` : '';
          await message.channel.send({ content: `Resolver failed: ${probe?.error || 'unknown'}${dbg}`.slice(0,1900) });
        }
      } catch {}
    }
  } catch (e) {
    console.error('jwtest error:', e);
    try { await message.reply('❌ jwtest failed.'); } catch {}
  }
}

async function handleJwSearchCommand(message, query) {
  try {
    if (!query) return void message.reply('❌ Usage: !jwsearch <name>');
    const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
    const res = await fetch(`${base}/juicewrld`, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' } }).catch(() => null);
    if (!res || !res.ok) return void message.reply('❌ API unreachable.');
    let data = null; try { data = await res.json(); } catch {}
    if (!data) return void message.reply('❌ Bad response.');
    let songs = Array.isArray(data.songs) ? data.songs : null;
    if (!songs) songs = findFirstArrayOfObjectsWithAnyKey(data, ['id','pk','title','name']) || [];
    if (!Array.isArray(songs) || songs.length === 0) return void message.reply('❌ No songs index found.');
    const scored = songs.map(s => {
      const name = s.title || s.name || s.track_title || s.song_title || '';
      return { s, name, score: tokenOverlapScore(name, query) };
    }).filter(x => x.name);
    scored.sort((a,b) => b.score - a.score);
    const top = scored.slice(0, 5);
    if (top.length === 0) return void message.reply('❌ No matches.');
    const lines = top.map(x => `• ${x.name} (id: ${x.s?.id ?? x.s?.pk ?? '?'}, score: ${x.score.toFixed(2)})`);
    await message.reply(`Top matches for "${query}":\n${lines.join('\n')}`);
  } catch (e) {
    console.error('jwsearch error:', e);
    try { await message.reply('❌ jwsearch failed.'); } catch {}
  }
}

async function handleJwPlayCommand(message, query) {
  try {
    if (!query) return void message.reply('❌ Usage: !jwplay <name>');
    if (!connection) {
      await handleJoinCommand(message);
      if (!connection) return; // join failed
    }
    // Lookup ID from /juicewrld index
    const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
    const res = await fetch(`${base}/juicewrld`, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' } }).catch(() => null);
    if (!res || !res.ok) return void message.reply('❌ API unreachable.');
    let data = null; try { data = await res.json(); } catch {}
    if (!data) return void message.reply('❌ Bad response.');
    let songs = Array.isArray(data.songs) ? data.songs : null;
    if (!songs) songs = findFirstArrayOfObjectsWithAnyKey(data, ['id','pk','title','name','track_title','song_title']) || [];
    if (!Array.isArray(songs) || songs.length === 0) return void message.reply('❌ No songs found.');
    const norm = (s) => (s || '').toString().toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    const qn = norm(query);
    let best = null; let bestScore = 0;
    for (const r of songs) {
      const nm = r?.title || r?.name || r?.track_title || r?.song_title || '';
      const score = tokenOverlapScore(nm, query);
      if (norm(nm) === qn || score > bestScore) { best = r; bestScore = score; }
    }
    if (!best) return void message.reply('❌ No match.');
    const idVal = best?.id ?? best?.pk ?? null;
    if (idVal == null) return void message.reply('❌ Match found but no ID field.');
    const playUrl = `${base}/player/songs/${encodeURIComponent(String(idVal))}/play/`;
    const title = best.title || best.name || best.track_title || best.song_title || query;
    songQueue.push({ type: 'external', url: playUrl, title, source: 'juicewrld' });
    if (!isPlaying) playNextSong();
    return void message.reply(`✅ Queued via JW API: ${title}`);
  } catch (e) {
    console.error('jwplay error:', e);
    try { await message.reply('❌ jwplay failed.'); } catch {}
  }
}

async function handleJwPlayIdCommand(message, idText) {
  try {
    const id = String(idText || '').trim();
    if (!/^\d+$/.test(id)) return void message.reply('❌ Usage: !jwplayid <numeric id>');
    if (!connection) {
      await handleJoinCommand(message);
      if (!connection) return; // join failed
    }
    const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
    const playUrl = `${base}/player/songs/${id}/play/`;
    songQueue.push({ type: 'external', url: playUrl, title: `JW #${id}`, source: 'juicewrld' });
    if (!isPlaying) playNextSong();
    return void message.reply(`✅ Queued via JW API: id ${id}`);
  } catch (e) {
    console.error('jwplayid error:', e);
    try { await message.reply('❌ jwplayid failed.'); } catch {}
  }
}

// --- REST fetch helper for JW /songs endpoint ---
async function fetchJuiceSongs({ search = '', page = 1, pageSize = 50, category = '' }) {
  const base = (config.JUICEWRLD_API_BASE || '').replace(/\/$/, '');
  const songsBase = (config.JUICEWRLD_SONGS_ENDPOINT && config.JUICEWRLD_SONGS_ENDPOINT.trim()) || `${base}/songs/`;
  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('page_size', String(pageSize));
  if (search) params.set('search', search);
  if (category) params.set('category', category);
  const suffix = `?${params.toString()}`;
  const urls = [
  `${songsBase}${suffix}`,
  `${stripTrailingSlash(songsBase)}.json${suffix}`,
    `${base}/api/songs/${suffix}`,
    `${base}/api/songs.json${suffix}`
  ];
  let last = { status: 0, url: urls[0], ctype: '' };
  for (const url of urls) {
    const res = await fetch(url, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0', 'Referer': base } }).catch(() => null);
    if (!res) { last = { status: 0, url, ctype: '' }; continue; }
    last = { status: res.status, url, ctype: String(res.headers.get('content-type') || '') };
    if (!res.ok) continue;
    let data = null; let parsedJson = false;
    try { data = await res.json(); parsedJson = true; } catch {}
    if (parsedJson && data && typeof data === 'object') {
      const results = Array.isArray(data.results) ? data.results : (Array.isArray(data) ? data : []);
      const count = data.count != null ? data.count : results.length;
      if (results.length) return { ok: true, url, count, next: data.next || null, previous: data.previous || null, results, ctype: last.ctype };
    }
    // HTML fallback: try to extract embedded JSON
    try {
      const text = parsedJson ? '' : await res.text();
      if (text && /<html/i.test(text)) {
        const embedded = extractEmbeddedJsonFromHtml(text);
        if (embedded) {
          const arr = findFirstArrayOfObjectsWithAnyKey(embedded, ['results','id','pk','name','title']);
          let results = [];
          if (Array.isArray(embedded.results)) results = embedded.results;
          else if (Array.isArray(arr)) results = arr;
          if (results.length) return { ok: true, url, count: results.length, next: null, previous: null, results, ctype: last.ctype };
        }
      }
    } catch {}
  }
  return { ok: false, status: last.status, url: last.url, ctype: last.ctype };
}

// !jwsongs: fetch songs via REST and list concise results
async function handleJwSongsCommand(message, argText) {
  try {
    const args = (argText || '').trim();
    // Parse flags like page=2 size=25 cat=released
    let search = args;
    let page = 1; let size = 10; let category = '';
    const flagRe = /(\bpage|size|pagesize|cat|category)=(\S+)/gi;
    let m; const taken = [];
    while ((m = flagRe.exec(args)) !== null) {
      const k = m[1].toLowerCase(); const v = m[2];
      taken.push(m[0]);
      if (k === 'page') page = Math.max(1, parseInt(v, 10) || 1);
      else if (k === 'size' || k === 'pagesize') size = Math.min(50, Math.max(1, parseInt(v, 10) || 10));
      else if (k === 'cat' || k === 'category') category = v;
    }
    if (taken.length) {
      const pattern = new RegExp(taken.map(x => x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'), 'gi');
      search = args.replace(pattern, '').trim();
    }
    if (!search) return void message.reply('❌ Usage: !jwsongs <search terms> [page=N] [size=M] [cat=released]');
  const r = await fetchJuiceSongs({ search, page, pageSize: size, category });
  if (!r.ok) return void message.reply(`❌ Request failed (${r.status || 0}) for ${r.url}${r.ctype?` [${r.ctype}]`:''}.`);
    if (!Array.isArray(r.results) || r.results.length === 0) return void message.reply('❌ No results.');
    const lines = r.results.slice(0, size).map(s => {
      const id = s.id ?? s.pk ?? '?';
      const name = s.name || s.title || s.track_title || s.song_title || '(untitled)';
      const cat = s.category || '';
      const era = s.era && s.era.name ? ` — ${s.era.name}` : '';
      const art = s.credited_artists ? ` — ${s.credited_artists}` : '';
      return `• [${id}] ${name}${art}${era}${cat?` (${cat})`:''}`;
    });
    const header = `JW /songs (page ${page}${r.next? ' → next' : ''}${r.previous? ' ← prev' : ''}) for "${search}"`;
    const content = [header, ...lines].join('\n');
    if (content.length <= 1900) {
      await message.reply({ content });
    } else {
      const txtPath = require('path').join(__dirname, 'jwsongs.txt');
      require('fs').writeFileSync(txtPath, content, 'utf8');
      await message.reply({ content: header, files: [txtPath] });
      try { require('fs').unlinkSync(txtPath); } catch {}
    }
  } catch (e) {
    console.error('jwsongs error:', e);
    try { await message.reply('❌ jwsongs failed.'); } catch {}
  }
}

// ================== Remote Catalog (simple alternative to local library) ==================
// Configure REMOTE_CATALOG_URL to point to a JSON array: [{ title: string, url: string }]
const REMOTE_CATALOG_URL = process.env.REMOTE_CATALOG_URL || fileConfig.REMOTE_CATALOG_URL || '';
let REMOTE_CATALOG = null; // cache after first load
async function loadRemoteCatalog() {
  if (!REMOTE_CATALOG_URL) return null;
  try {
    const res = await fetch(REMOTE_CATALOG_URL, { headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' } }).catch(() => null);
    if (!res || !res.ok) return null;
    const data = await res.json().catch(() => null);
    if (!Array.isArray(data)) return null;
    REMOTE_CATALOG = data.filter(x => x && typeof x.url === 'string');
    return REMOTE_CATALOG;
  } catch { return null; }
}
async function searchRemoteCatalog(q) {
  if (!REMOTE_CATALOG) await loadRemoteCatalog();
  const arr = Array.isArray(REMOTE_CATALOG) ? REMOTE_CATALOG : [];
  const scored = arr.map(x => ({ x, score: tokenOverlapScore(x.title || x.name || x.url || '', q) }))
    .filter(s => s.score > 0)
    .sort((a,b) => b.score - a.score);
  return scored.map(s => s.x);
}

async function handleRcPlayCommand(message, query) {
  try {
    const q = String(query || '').trim();
    if (!q) return void message.reply('❌ Usage: !rcplay <name>');
    if (!connection) { await handleJoinCommand(message); if (!connection) return; }
    const list = await searchRemoteCatalog(q);
    if (!list || list.length === 0) return void message.reply('❌ No matches in remote catalog.');
    const hit = list[0];
    if (!isUrl(hit.url)) return void message.reply('❌ Invalid URL in catalog entry.');
    const title = hit.title || hit.name || 'Remote Audio';
    songQueue.push({ type: 'external', url: hit.url, title, source: 'external' });
    if (!isPlaying) playNextSong();
    return void message.reply(`✅ Queued from remote catalog: ${title}`);
  } catch (e) {
    console.error('rcplay error:', e);
    try { await message.reply('❌ rcplay failed.'); } catch {}
  }
}

async function handleRcListCommand(message, query) {
  try {
    const q = String(query || '').trim();
    if (!q) return void message.reply('❌ Usage: !rclist <query>');
    const list = await searchRemoteCatalog(q);
    if (!list || list.length === 0) return void message.reply('❌ No matches.');
    const lines = list.slice(0, 10).map((x, i) => `• ${x.title || x.name || x.url}`);
    return void message.reply(lines.join('\n'));
  } catch (e) {
    console.error('rclist error:', e);
    try { await message.reply('❌ rclist failed.'); } catch {}
  }
}

// ================== MEGA streaming support ==================
async function handleMegaLoginCommand(message, argText) {
  try {
    const args = String(argText || '').trim();
    // Accept either: email:pass or an exported folder link (public folder)
  if (!MegaFile && !MegaStorage) return void message.reply('❌ MEGA support not installed.');
    if (!args) {
      if (config.MEGA_FOLDER_LINK) {
        const normalized = normalizeMegaFolderLink(config.MEGA_FOLDER_LINK);
        if (!normalized) return void message.reply('❌ MEGA_FOLDER_LINK is missing its decryption key (#...). Add the key or set MEGA_FOLDER_KEY and retry.');
  const parts = splitMegaFolder(normalized);
  if (parts) try { console.log(`🔎 megalogin env: id=${mask(parts.id)} key=${mask(parts.key)}`); } catch {}
        megaFilesIndex = new Map();
        const count = await indexMegaFromFolderLink(normalized);
        if (count > 0) return void message.reply(`✅ MEGA indexed from env (MEGA_FOLDER_LINK): ${count} files.`);
        return void message.reply('❌ Failed to index MEGA from env. Check MEGA_FOLDER_LINK or try !megalogin <folderLink>.');
      }
      return void message.reply('❌ Usage: !megalogin <email:password | folderLink> (or set MEGA_FOLDER_LINK and run !megalogin)');
    }
    if (/^https?:\/\/mega\.nz\//i.test(args)) {
      if (!MegaFile || !MegaFile.fromURL) return void message.reply('❌ MEGA module missing file support.');
      // Public folder link: recursively index with MegaFile
      megaFilesIndex = new Map();
      const count = await indexMegaFromFolderLink(args);
      if (count <= 0) return void message.reply('❌ Failed to index MEGA folder link.');
      mega = null; // no session needed for public folders
    } else {
      const [email, password] = args.split(':');
      if (!email || !password) return void message.reply('❌ Provide email:password or a folder link.');
      if (!MegaStorage) return void message.reply('❌ MEGA login not available.');
      mega = new MegaStorage({ email, password });
      await new Promise((res, rej) => mega.login((err) => err ? rej(err) : res())).catch(() => null);
      if (!mega) return void message.reply('❌ MEGA login failed.');
      // Build index from logged-in storage tree
      megaFilesIndex = new Map();
      const stack = Array.isArray(mega.files) ? [...mega.files] : [];
      while (stack.length) {
        const f = stack.pop();
        if (!f) continue;
        if (f.children && Array.isArray(f.children)) stack.push(...f.children);
        if (f.directory) continue;
        const name = f.name || '';
        if (!isSupportedAudioName(name)) continue;
  const link = f.downloadLink || (typeof f.link === 'function' ? f.link() : '');
  if (name) megaFilesIndex.set(name.toLowerCase(), { link: link || '', file: f });
      }
    }
    return void message.reply(`✅ MEGA ready. Indexed ${megaFilesIndex.size} files.`);
  } catch (e) {
    console.error('megalogin error:', e);
    try { await message.reply('❌ megalogin failed.'); } catch {}
  }
}

async function handleMegaListCommand(message) {
  try {
  if (megaFilesIndex.size === 0) { await ensureMegaReadyFromEnv(); }
  if (megaFilesIndex.size === 0) return void message.reply('❌ MEGA not initialized. Set MEGA_FOLDER_LINK and run !megalogin (no args), or use !megalogin <folderLink>.');
  const names = Array.from(megaFilesIndex.keys()).slice(0, 30);
    return void message.reply('MEGA files:\n' + names.map(n => `• ${n}`).join('\n'));
  } catch (e) {
    console.error('megalist error:', e);
    try { await message.reply('❌ megalist failed.'); } catch {}
  }
}

async function handleMegaPlayCommand(message, argText) {
  try {
    let q = String(argText || '').trim();
    if (!q) return void message.reply('❌ Usage: !megaplay <mega file link | file name>');
    if (!connection) { await handleJoinCommand(message); if (!connection) return; }
    // If it looks like a MEGA file link, we can stream it directly via megajs
    let stream = null;
    let title = '';
    if (/^https?:\/\/mega\.nz\//i.test(q)) {
      if (!MegaFile) return void message.reply('❌ MEGA support not installed.');
      const file = MegaFile.fromURL(q);
      title = file.name || 'MEGA Audio';
  stream = null;
  try { if (typeof file.download === 'function') stream = file.download({ start: 0 }); } catch {}
  if (!stream) { try { if (typeof file.createReadStream === 'function') stream = file.createReadStream({ initialChunkSize: 1024 * 1024, chunkSize: 1024 * 1024 }); } catch {} }
    } else {
    if (megaFilesIndex.size === 0) { await ensureMegaReadyFromEnv(); }
    if (megaFilesIndex.size === 0) return void message.reply('❌ MEGA not initialized. Set MEGA_FOLDER_LINK and run !megalogin (no args), or use !megalogin <folderLink>.');
      const entry = findMegaEntryByName(q);
      if (!entry) return void message.reply('❌ File not found in MEGA index. Try !megalist.');
  const file = entry.file || (entry.link && MegaFile.fromURL(entry.link));
  if (!file) return void message.reply('❌ Could not resolve MEGA file.');
  title = file.name || q;
  stream = null;
  try { if (typeof file.download === 'function') stream = file.download({ start: 0 }); } catch {}
  if (!stream) { try { if (typeof file.createReadStream === 'function') stream = file.createReadStream({ initialChunkSize: 1024 * 1024, chunkSize: 1024 * 1024 }); } catch {} }
    }
    if (!stream) {
      // Fallback: if this originated from a link, try piping the URL via ffmpeg directly
      if (/^https?:\/\/mega\.nz\//i.test(q)) {
        songQueue.push({ type: 'external', url: q, title: title || 'MEGA Audio', source: 'external' });
        if (!isPlaying) playNextSong();
        return void message.reply(`✅ Queued from MEGA (URL fallback): ${title || 'MEGA Audio'}`);
      }
      return void message.reply('❌ Could not open MEGA stream.');
    }
  // Pipe stream via ffmpeg (normalize to s16le) using the shared helper with diagnostics
  const resource = createFfmpegResourceFromReadable(stream);
    songQueue.push({ resource, title: title || 'MEGA Audio', source: 'external' });
    if (!isPlaying) playNextSong();
    return void message.reply(`✅ Queued from MEGA: ${title}`);
  } catch (e) {
    console.error('megaplay error:', e);
    try { await message.reply('❌ megaplay failed.'); } catch {}
  }
}

async function indexMegaFromFolderLink(url) {
  try {
  const normalized = normalizeMegaFolderLink(url);
  if (!normalized) throw new Error('Invalid MEGA URL: missing decryption key (hash).');
  const root = MegaFile.fromURL(normalized);
    await new Promise((res, rej) => root.loadAttributes((err) => err ? rej(err) : res()));
    let count = 0;
    async function walk(node) {
      if (!node) return;
      if (node.directory) {
        // Ensure children loaded
        if (!Array.isArray(node.children)) {
          await new Promise((res) => node.loadAttributes(() => res()));
        }
        const kids = Array.isArray(node.children) ? node.children : [];
        for (const k of kids) await walk(k);
      } else {
        const name = node.name || '';
  if (name && isSupportedAudioName(name)) {
          megaFilesIndex.set(name.toLowerCase(), { file: node });
          count++;
        }
      }
    }
    await walk(root);
    return count;
  } catch (e) {
    try {
      const parts = splitMegaFolder(url);
      if (parts) console.warn(`MEGA folder index failed for id=${mask(parts.id)} key=${mask(parts.key)}:`, e?.message || e);
      else console.warn('MEGA folder index failed:', e?.message || e);
    } catch {}
    return 0;
  }
}

async function handleMegaInfoCommand(message) {
  try {
    const normalized = normalizeMegaFolderLink(config.MEGA_FOLDER_LINK || '');
    const parts = normalized ? splitMegaFolder(normalized) : null;
    if (!parts) return void message.reply('ℹ️ MEGA not configured or missing key. Set MEGA_FOLDER_LINK (and MEGA_FOLDER_KEY if needed).');
    return void message.reply(`MEGA config:\n• id=${mask(parts.id)}\n• key=${mask(parts.key)}\n• indexed=${megaFilesIndex.size}`);
  } catch {
    return void message.reply('ℹ️ MEGA info unavailable.');
  }
}

async function handleAudioTestCommand(message) {
  try {
    if (!connection) { await handleJoinCommand(message); if (!connection) return; }
    // Generate a 440Hz sine for 5s using ffmpeg and play it
    const args = ['-f', 'lavfi', '-i', 'sine=frequency=440:duration=5', '-f', 's16le', '-ar', '48000', '-ac', '2', 'pipe:1'];
    const proc = spawn(ffmpegPath, args, { stdio: ['ignore', 'pipe', 'ignore'] });
    const resource = createAudioResource(proc.stdout, { inlineVolume: true, inputType: StreamType.Raw });
    songQueue.unshift({ resource, title: 'Test Tone (440Hz)', source: 'test' });
    if (!isPlaying) playNextSong();
    return void message.reply('🔊 Playing 5s test tone. Do you hear it?');
  } catch (e) {
    console.error('audiotest error:', e);
    return void message.reply('❌ Failed to start test tone.');
  }
}

async function handleVcDiagCommand(message) {
  try {
    const state = player?.state?.status || 'unknown';
    const vol = player?.state?.resource?.volume?.volume ?? null;
    const ch = getConnectedVoiceChannel();
    const listeners = ch?.members ? ch.members.filter(m => !m.user.bot).size : 0;
    const label = `Player: ${state}${vol!=null?`, volume=${Math.round(vol*100)}%`:''}\nChannel: ${ch ? ch.name : 'none'}\nListeners (non-bot): ${listeners}`;
    return void message.reply(label);
  } catch (e) {
    return void message.reply('❌ vCDiag failed.');
  }
}