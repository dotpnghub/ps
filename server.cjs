'use strict';

const http = require('node:http'), fs = require('node:fs');
const path = require('node:path'), crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { OBSWebSocket } = require('obs-websocket-js');
const QR = require('qrcode');
const FFMPEG = process.env.SPORTS_FFMPEG || require('ffmpeg-static');

const PORT = 8787, BASE = 'http://127.0.0.1:' + PORT;
const DIR = path.join(__dirname, 'sports-data');

for (const d of ['', 'assets', 'clips']) {
  fs.mkdirSync(path.join(DIR, d), { recursive: true });
}

const readJSON = (name, fallback) =>
  fs.existsSync(path.join(DIR, name))
    ? JSON.parse(fs.readFileSync(path.join(DIR, name), 'utf8'))
    : fallback;

function writeJSON(name, value) {
  const f = path.join(DIR, name);
  fs.writeFileSync(f + '.tmp', JSON.stringify(value, null, 2));
  fs.renameSync(f + '.tmp', f);
}

const copy = x => JSON.parse(JSON.stringify(x));

function number(x, min, max) {
  x = Number(x);
  if (!Number.isFinite(x)) throw Error('ตัวเลขไม่ถูกต้อง');
  return Math.min(max, Math.max(min, x));
}

const short = (x, n = 100) =>
  String(x == null ? '' : x).slice(0, n);

function clock(ms = 0, down = false) {
  return { ms, down, running: false, at: 0 };
}

function clockValue(c, now = Date.now()) {
  return Math.max(
    0,
    c.ms + (c.running ? (now - c.at) * (c.down ? -1 : 1) : 0)
  );
}

function pause(c) {
  c.ms = clockValue(c);
  c.running = false;
  c.at = 0;
}

function toggle(c) {
  if (c.running) return pause(c);
  if (c.down && c.ms <= 0) return;
  c.at = Date.now();
  c.running = true;
}

function freshSport(mode) {
  return {
    score: [0, 0],
    fouls: [0, 0],
    sets: [0, 0],
    penalties: [[], []],
    setHistory: [],
    serve: 0,
    period: '1',
    clock: clock(
      mode === 'basketball' ? 600000 : 0,
      mode === 'basketball'
    ),
    shot: clock(24000, true)
  };
}

const initial = {
  mode: 'football',
  title: 'SPORTS LIVE',
  bg: '#075580',
  text: '#ffffff',
  scale: 1,
  x: 50,
  eventScale: 1,
  showTime: true,
  showFouls: false,
  showPenalties: false,
  stinger: false,
  logo: '',
  teams: [
    { name: 'ทีมเจ้าบ้าน', color: '#e4f026', logo: '' },
    { name: 'ทีมเยือน', color: '#a822ef', logo: '' }
  ],
  sports: Object.fromEntries(
    ['football', 'basketball', 'volleyball'].map(m => [m, freshSport(m)])
  ),
  event: null,
  cameras: []
};

let state = readJSON('state.json', initial);

for (const s of Object.values(state.sports)) {
  s.clock.running = false;
  s.shot.running = false;
}

state.event = null;

let config = readJSON('obs.json', { port: 4455, password: '' });
let clips = readJSON('clips.json', []);

let status = {
  connected: false,
  buffer: false,
  program: '',
  phase: 'idle',
  message: 'ยังไม่เชื่อม OBS'
};

let animation = 0;
let history = [];
let peers = new Set();
let connected = false;
let active = null;
let job = null;
let generation = 0;
let child = null;
let connecting = false;

const obs = new OBSWebSocket();

const LIVE = 'SPORTS_LIVE';
const REPLAY = 'SPORTS_REPLAY';
const VS = 'SPORTS_VS';
const MEDIA = 'SPORTS_REPLAY_VIDEO';
const GRAPHICS = 'SPORTS_GRAPHICS';

const clipPath = id => path.join(DIR, 'clips', id + '.mp4');

function checkpoint() {
  const s = copy(state), now = Date.now();

  for (const v of Object.values(s.sports)) {
    for (const k of ['clock', 'shot']) {
      v[k].ms = clockValue(v[k], now);
      v[k].at = now;
    }
  }

  writeJSON('state.json', s);
  writeJSON('clips.json', clips);
}

function payload() {
  return {
    state,
    clips,
    status,
    animation,
    obsPort: config.port,
    now: Date.now()
  };
}

function broadcast() {
  const msg = 'data: ' + JSON.stringify(payload()) + '\n\n';

  for (const p of peers) {
    if (!p.write(msg)) {
      p.end();
      peers.delete(p);
    }
  }
}

function changed() {
  checkpoint();
  broadcast();
}

function phase(p, message) {
  status.phase = p;
  status.message = message;
  broadcast();
}

function remember() {
  history.push({
    mode: state.mode,
    value: copy(state.sports[state.mode])
  });

  if (history.length > 30) history.shift();
}

function scoreAction(b) {
  let s = state.sports[state.mode], t = Number(b.team);

  if (
    ['score', 'foul', 'penalty', 'serve', 'set'].includes(b.type) &&
    ![0, 1].includes(t)
  ) {
    throw Error('ทีมไม่ถูกต้อง');
  }

  switch (b.type) {
    case 'mode':
      if (!state.sports[b.mode]) throw Error('กีฬาไม่ถูกต้อง');

      for (const v of Object.values(state.sports)) {
        pause(v.clock);
        pause(v.shot);
      }

      state.mode = b.mode;
      state.event = null;
      break;

    case 'style':
      for (const k of ['title']) {
        if (b[k] != null) state[k] = short(b[k]);
      }

      for (const k of ['bg', 'text']) {
        if (/^#[0-9a-f]{6}$/i.test(b[k] || '')) state[k] = b[k];
      }

      for (const k of [
        'showTime', 'showFouls', 'showPenalties', 'stinger'
      ]) {
        if (typeof b[k] === 'boolean') state[k] = b[k];
      }

      if (b.scale != null) state.scale = number(b.scale, 0.5, 1.8);
      if (b.x != null) state.x = number(b.x, 20, 80);
      if (b.eventScale != null) {
        state.eventScale = number(b.eventScale, 0.5, 1.8);
      }
      break;

    case 'team':
      if (![0, 1].includes(t)) throw Error('ทีมไม่ถูกต้อง');
      state.teams[t].name = short(b.name, 60);

      if (/^#[0-9a-f]{6}$/i.test(b.color || '')) {
        state.teams[t].color = b.color;
      }
      break;

    case 'score':
    case 'foul':
      remember();
      {
        const k = b.type === 'score' ? 'score' : 'fouls';
        s[k][t] = number(
          s[k][t] + Math.trunc(number(b.delta, -3, 3)),
          0,
          999
        );
      }
      break;

    case 'penalty':
      remember();

      if (b.undo) s.penalties[t].pop();
      else if (s.penalties[t].length < 30) {
        s.penalties[t].push(Boolean(b.goal));
      }
      break;

    case 'clearPenalty':
      remember();
      s.penalties = [[], []];
      break;

    case 'serve':
      s.serve = t;
      break;

    case 'set':
      if (state.mode !== 'volleyball') {
        throw Error('ใช้กับวอลเลย์บอล');
      }

      remember();
      s.setHistory.push(s.score.slice());
      s.sets[t]++;
      s.score = [0, 0];
      s.period = String(s.setHistory.length + 1);
      break;

    case 'period':
      s.period = short(b.value, 20);
      break;

    case 'clockToggle':
      toggle(s.clock);
      if (!s.clock.running) pause(s.shot);
      break;

    case 'clockSet':
      s.clock = clock(
        number(b.seconds, 0, 86400) * 1000,
        Boolean(b.down)
      );
      pause(s.shot);
      break;

    case 'shotToggle':
      if (!s.clock.running) {
        throw Error('เริ่มเวลาแข่งขันก่อนเริ่ม Shot Clock');
      }
      toggle(s.shot);
      break;

    case 'shotSet':
      s.shot = clock(number(b.seconds, 0, 99) * 1000, true);
      break;

    case 'reset':
      remember();
      state.sports[state.mode] = freshSport(state.mode);
      state.event = null;
      break;

    case 'undo': {
      const h = history.pop();

      if (h) {
        const clocks = {
          clock: state.sports[h.mode].clock,
          shot: state.sports[h.mode].shot
        };
        state.sports[h.mode] = Object.assign(h.value, clocks);
      }
      break;
    }

    case 'event':
      if (![0, 1].includes(t)) throw Error('ทีมไม่ถูกต้อง');

      state.event = {
        team: t,
        label: short(b.label, 40),
        player: short(b.player, 70),
        number: short(b.number, 8),
        until: Date.now() + number(b.seconds || 8, 2, 60) * 1000
      };
      break;

    case 'hideEvent':
      state.event = null;
      break;

    default:
      throw Error('ไม่รู้จักคำสั่ง');
  }

  changed();
}

const wait = ms => new Promise(r => setTimeout(r, ms));

async function call(type, data = {}) {
  if (!connected) throw Error('กรุณาเชื่อมต่อ OBS');
  return obs.call(type, data);
}

async function connect(b = {}) {
  if (connecting || job || active) {
    throw Error('กำลังเชื่อมต่อหรือเล่น Replay');
  }

  connecting = true;

  try {
    await obs.disconnect().catch(() => {});

    config.port = Math.trunc(number(b.port || config.port, 1, 65535));

    if (b.password !== undefined && b.password !== '') {
      config.password = short(b.password, 200);
    }

    await obs.connect(
      'ws://127.0.0.1:' + config.port,
      config.password
    );

    connected = status.connected = true;
    writeJSON('obs.json', config);

    status.buffer = (await call('GetReplayBufferStatus')).outputActive;
    status.program =
      (await call('GetCurrentProgramScene')).currentProgramSceneName;

    phase('idle', 'เชื่อมต่อ OBS แล้ว');
  } finally {
    connecting = false;
  }
}

obs.on('ConnectionError', e => {
  status.message = short(e.message, 200);
  broadcast();
});

obs.on('ConnectionClosed', () => {
  connected = status.connected = false;
  status.buffer = false;
  generation++;

  if (child) child.kill();
  active = null;

  phase('idle', 'การเชื่อมต่อ OBS ขาด — กดเชื่อมต่อใหม่');
});

obs.on('ReplayBufferStateChanged', e => {
  status.buffer = e.outputActive;
  broadcast();
});

obs.on('CurrentProgramSceneChanged', e => {
  status.program = e.sceneName;

  if (
    active &&
    status.phase === 'playing' &&
    e.sceneName !== REPLAY
  ) {
    active = null;
    generation++;
    phase('idle', 'ผู้ควบคุมเปลี่ยน Scene แล้ว');

    call('TriggerMediaInputAction', {
      inputName: MEDIA,
      mediaAction: 'OBS_WEBSOCKET_MEDIA_INPUT_ACTION_STOP'
    }).catch(() => {});
  }

  broadcast();
});

obs.on('MediaInputPlaybackStarted', e => {
  if (e.inputName === MEDIA && active) active.started = true;
});

obs.on('MediaInputPlaybackEnded', e => {
  if (
    e.inputName === MEDIA &&
    active &&
    active.started &&
    status.phase === 'playing'
  ) {
    goLive().catch(report);
  }
});

function report(e) {
  phase('idle', e.message || String(e));
}

async function ensureScene(name) {
  const r = await call('GetSceneList');

  if (!r.scenes.some(s => s.sceneName === name)) {
    await call('CreateScene', { sceneName: name });
  }
}

async function ensureInput(
  scene,
  name,
  kind,
  settings,
  bottom = false
) {
  await ensureScene(scene);

  const list = await call('GetInputList');
  const old = list.inputs.find(i => i.inputName === name);

  if (
    old &&
    old.inputKind !== kind &&
    old.unversionedInputKind !== kind
  ) {
    throw Error('ชื่อ Source ซ้ำกับชนิดอื่น: ' + name);
  }

  if (!old) {
    await call('CreateInput', {
      sceneName: scene,
      inputName: name,
      inputKind: kind,
      inputSettings: settings,
      sceneItemEnabled: true
    });
  } else {
    await call('SetInputSettings', {
      inputName: name,
      inputSettings: settings,
      overlay: true
    });

    const items =
      (await call('GetSceneItemList', { sceneName: scene })).sceneItems;

    if (!items.some(i => i.sourceName === name)) {
      await call('CreateSceneItem', {
        sceneName: scene,
        sourceName: name,
        sceneItemEnabled: true
      });
    }
  }

  const id = (
    await call('GetSceneItemId', {
      sceneName: scene,
      sourceName: name
    })
  ).sceneItemId;

  const v = await call('GetVideoSettings');

  await call('SetSceneItemTransform', {
    sceneName: scene,
    sceneItemId: id,
    sceneItemTransform: {
      positionX: 0,
      positionY: 0,
      alignment: 5,
      boundsAlignment: 0,
      boundsType: 'OBS_BOUNDS_SCALE_INNER',
      boundsWidth: v.baseWidth,
      boundsHeight: v.baseHeight
    }
  });

  if (bottom) {
    await call('SetSceneItemIndex', {
      sceneName: scene,
      sceneItemId: id,
      sceneItemIndex: 0
    });
  }
}

function browserSettings(url) {
  return {
    url,
    width: 1920,
    height: 1080,
    fps: 30,
    shutdown: false,
    restart_when_active: false
  };
}

async function setup() {
  await ensureInput(
    LIVE,
    GRAPHICS,
    'browser_source',
    browserSettings(BASE + '/?view=overlay')
  );

  await ensureInput(
    VS,
    'SPORTS_VERSUS',
    'browser_source',
    browserSettings(BASE + '/?view=versus')
  );

  await ensureInput(REPLAY, MEDIA, 'ffmpeg_source', {
    is_local_file: true,
    local_file: '',
    looping: false,
    restart_on_activate: false,
    close_when_inactive: false,
    clear_on_media_end: true,
    speed_percent: 100
  });

  await ensureInput(
    REPLAY,
    'SPORTS_REPLAY_GRAPHICS',
    'browser_source',
    browserSettings(BASE + '/?view=replay')
  );

  for (const cam of state.cameras) await installCamera(cam);

  phase('idle', 'สร้าง Scenes แล้ว — เพิ่มกล้องหรือกด LIVE');
}

function cameraName(c) {
  return 'SPORTS_CAM_' + c.id.slice(0, 8);
}

function cameraURLs(c) {
  const q = new URLSearchParams({
    password: c.password,
    label: c.name
  });

  return {
    push: 'https://vdo.ninja/?push=' + c.stream + '&' + q,
    view: 'https://vdo.ninja/?view=' + c.stream + '&' + q
  };
}

async function installCamera(c) {
  const settings = Object.assign(
    browserSettings(cameraURLs(c).view),
    { reroute_audio: true }
  );

  await ensureInput(
    LIVE,
    cameraName(c),
    'browser_source',
    settings,
    true
  );

  const items =
    (await call('GetSceneItemList', { sceneName: LIVE })).sceneItems;

  const id = items.find(i => i.sourceName === cameraName(c)).sceneItemId;

  await call('SetSceneItemEnabled', {
    sceneName: LIVE,
    sceneItemId: id,
    sceneItemEnabled: c.id === state.cameras[0].id
  });
}

async function takeCamera(id) {
  const c = state.cameras.find(x => x.id === id);
  if (!c) throw Error('ไม่พบกล้อง');

  const items =
    (await call('GetSceneItemList', { sceneName: LIVE })).sceneItems;

  const names = new Set(state.cameras.map(cameraName));

  if (!items.some(i => i.sourceName === cameraName(c))) {
    throw Error('กดสร้าง/อัปเดต Scenes ก่อน');
  }

  for (const i of items) {
    if (names.has(i.sourceName)) {
      await call('SetSceneItemEnabled', {
        sceneName: LIVE,
        sceneItemId: i.sceneItemId,
        sceneItemEnabled: i.sourceName === cameraName(c)
      });
    }
  }

  await goLive(LIVE);
}

function assertGeneration(n) {
  if (n !== generation) throw Error('ยกเลิกงาน Replay แล้ว');
}

async function exclusive(fn) {
  if (job || active) {
    throw Error('กำลังทำงาน Replay — กด LIVE เพื่อยกเลิกก่อน');
  }

  const n = ++generation;
  job = n;

  try {
    return await fn(n);
  } catch (e) {
    if (n === generation) phase('idle', e.message);
    throw e;
  } finally {
    if (job === n) job = null;
  }
}

function runFF(args, n, probeOnly = false) {
  return new Promise((resolve, reject) => {
    if (n != null) {
      try {
        assertGeneration(n);
      } catch (e) {
        return reject(e);
      }
    }

    const p = spawn(FFMPEG, args, {
      windowsHide: true,
      stdio: ['ignore', 'ignore', 'pipe']
    });

    if (n != null) child = p;

    let log = '', expired = false;

    const timer = setTimeout(() => {
      expired = true;
      p.kill();
    }, 180000);

    p.stderr.on('data', d => {
      log = (log + d).slice(-32000);
    });

    p.on('error', e => {
      clearTimeout(timer);
      if (child === p) child = null;
      reject(e);
    });

    p.on('close', code => {
      clearTimeout(timer);
      if (child === p) child = null;

      if (n != null && n !== generation) {
        return reject(Error('ยกเลิกงาน Replay แล้ว'));
      }

      if (expired) {
        return reject(Error('FFmpeg ใช้เวลานานเกินกำหนด'));
      }

      if (code !== 0 && !probeOnly) {
        return reject(Error(log.slice(-1200)));
      }

      resolve(log);
    });
  });
}

function durationFromLog(log) {
  const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(log);

  if (!m) throw Error('อ่านความยาวคลิปไม่ได้');

  return Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]);
}

function savedReplay() {
  let listener, timer;

  const promise = new Promise((resolve, reject) => {
    listener = e => {
      clearTimeout(timer);
      obs.off('ReplayBufferSaved', listener);
      resolve(e.savedReplayPath);
    };

    obs.on('ReplayBufferSaved', listener);

    timer = setTimeout(() => {
      obs.off('ReplayBufferSaved', listener);
      reject(Error('OBS ยังไม่ส่งไฟล์ Replay ภายใน 20 วินาที'));
    }, 20000);
  });

  return {
    promise,
    cancel() {
      clearTimeout(timer);
      obs.off('ReplayBufferSaved', listener);
    }
  };
}

async function capture(n) {
  if (!(await call('GetReplayBufferStatus')).outputActive) {
    throw Error('กด Start Buffer และรอสะสมภาพก่อน');
  }

  if (
    (await call('GetCurrentProgramScene')).currentProgramSceneName ===
    REPLAY
  ) {
    throw Error('กลับ LIVE ก่อนบันทึก Replay ใหม่');
  }

  phase('saving', 'กำลังบันทึก Replay Buffer…');

  const waiter = savedReplay();
  let original;

  try {
    const results = await Promise.all([
      call('SaveReplayBuffer'),
      waiter.promise
    ]);
    original = results[1];
  } finally {
    waiter.cancel();
  }

  assertGeneration(n);

  if (
    !fs.existsSync(original) ||
    !/\.(mkv|mp4|mov|flv|ts)$/i.test(original)
  ) {
    throw Error('ไม่พบไฟล์ Replay บนคอมเครื่องนี้');
  }

  const id = crypto.randomUUID(), dest = clipPath(id);

  phase('preparing', 'กำลังเตรียมไฟล์สำหรับ Preview…');

  try {
    const metadata = await runFF(
      ['-hide_banner', '-i', original],
      n,
      true
    );

    const codec = /Video:\s*h264/i.test(metadata)
      ? ['-c:v', 'copy']
      : [
          '-c:v', 'libx264',
          '-preset', 'veryfast',
          '-crf', '20',
          '-pix_fmt', 'yuv420p'
        ];

    await runFF([
      '-hide_banner', '-y',
      '-i', original,
      '-map', '0:v:0',
      '-map', '0:a:0?',
      ...codec,
      '-c:a', 'aac',
      '-avoid_negative_ts', 'make_zero',
      '-movflags', '+faststart',
      dest
    ], n);

    const info = await runFF(
      ['-hide_banner', '-i', dest],
      n,
      true
    );

    const clip = {
      id,
      name: 'Replay ' + new Date().toLocaleString('th-TH'),
      duration: durationFromLog(info),
      audio: /Audio:/.test(info),
      created: Date.now()
    };

    clips.unshift(clip);
    changed();
    phase('idle', 'บันทึกคลิปแล้ว');

    return clip;
  } catch (e) {
    try { fs.unlinkSync(dest); } catch {}
    throw e;
  }
}

function selection(clip, b) {
  const start = number(b.start || 0, 0, clip.duration);

  const end = number(
    b.end == null ? clip.duration : b.end,
    0,
    clip.duration
  );

  const speed = Number(b.speed == null ? 1 : b.speed);

  if (end - start < 0.1 || ![0.5, 0.75, 1].includes(speed)) {
    throw Error('เลือก In < Out และความเร็ว 50/75/100%');
  }

  return { start, end, speed };
}

async function playClip(clip, b, n) {
  const { start, end, speed } = selection(clip, b);

  const file = path.join(
    DIR,
    'clips',
    'play-' + crypto.randomUUID() + '.mp4'
  );

  phase(
    'preparing',
    'กำลังตัดช่วงและเตรียมความเร็ว ' +
      Math.round(speed * 100) + '%…'
  );

  try {
    const audioArgs = b.audio && clip.audio
      ? [
          '-map', '0:a:0',
          '-af', 'asetpts=PTS-STARTPTS,atempo=' + speed,
          '-c:a', 'aac'
        ]
      : ['-an'];

    await runFF([
      '-hide_banner', '-y',
      '-ss', start.toFixed(3),
      '-t', (end - start).toFixed(3),
      '-i', clipPath(clip.id),
      '-map', '0:v:0',
      '-vf', 'setpts=(PTS-STARTPTS)/' + speed,
      '-fps_mode', 'cfr',
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-crf', '20',
      '-pix_fmt', 'yuv420p',
      ...audioArgs,
      '-movflags', '+faststart',
      file
    ], n);

    assertGeneration(n);

    const current =
      (await call('GetCurrentProgramScene')).currentProgramSceneName;

    await call('SetInputSettings', {
      inputName: MEDIA,
      inputSettings: {
        is_local_file: true,
        local_file: file,
        looping: false,
        speed_percent: 100,
        restart_on_activate: false,
        close_when_inactive: false,
        clear_on_media_end: true
      },
      overlay: true
    });

    let ready = false;

    for (let i = 0; i < 50; i++) {
      assertGeneration(n);

      const media = await call('GetMediaInputStatus', {
        inputName: MEDIA
      });

      if (media.mediaDuration > 0) {
        ready = true;
        break;
      }

      await wait(100);
    }

    if (!ready) throw Error('OBS ยังเปิดคลิปไม่สำเร็จ');

    await call('TriggerMediaInputAction', {
      inputName: MEDIA,
      mediaAction: 'OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PAUSE'
    });

    await call('SetMediaInputCursor', {
      inputName: MEDIA,
      mediaCursor: 0
    });

    await call('SetInputMute', {
      inputName: MEDIA,
      inputMuted: !b.audio
    });

    assertGeneration(n);

    active = {
      n,
      file,
      back: current === REPLAY ? LIVE : current,
      started: false,
      clipId: clip.id
    };

    if (state.stinger) {
      animation = Date.now();
      broadcast();
      await wait(450);
    }

    assertGeneration(n);
    phase('arming', 'กำลังนำ Replay ออกอากาศ…');

    await call('SetCurrentProgramScene', { sceneName: REPLAY });

    assertGeneration(n);

    await call('TriggerMediaInputAction', {
      inputName: MEDIA,
      mediaAction: 'OBS_WEBSOCKET_MEDIA_INPUT_ACTION_RESTART'
    });

    if (!active || active.n !== n) {
      throw Error('ยกเลิก Replay แล้ว');
    }

    phase('playing', 'REPLAY • ' + Math.round(speed * 100) + '%');
  } catch (e) {
    if (active && active.n === n) {
      await goLive().catch(() => {});
    } else {
      try { fs.unlinkSync(file); } catch {}
    }

    throw e;
  }
}

async function goLive(target) {
  const previous = active;

  generation++;
  if (child) child.kill();
  active = null;

  phase('returning', 'กำลังกลับภาพสด…');

  try {
    await call('SetCurrentProgramScene', {
      sceneName: target || (previous && previous.back) || LIVE
    });
  } catch (e) {
    active = previous;
    phase(previous ? 'playing' : 'idle', e.message);
    throw e;
  }

  await call('TriggerMediaInputAction', {
    inputName: MEDIA,
    mediaAction: 'OBS_WEBSOCKET_MEDIA_INPUT_ACTION_STOP'
  }).catch(() => {});

  await call('SetInputSettings', {
    inputName: MEDIA,
    inputSettings: { local_file: '' },
    overlay: true
  }).catch(() => {});

  if (previous) {
    try { fs.unlinkSync(previous.file); } catch {}
  }

  phase('idle', target === VS ? 'แสดง VS' : 'LIVE');
}

let polling = false;

setInterval(async () => {
  if (!active || status.phase !== 'playing' || polling) return;

  polling = true;

  try {
    const a = active;
    const r = await call('GetMediaInputStatus', { inputName: MEDIA });

    if (active === a && r.mediaState === 'OBS_MEDIA_STATE_PLAYING') {
      a.started = true;
    }

    if (
      active === a &&
      a.started &&
      r.mediaState === 'OBS_MEDIA_STATE_ENDED'
    ) {
      await goLive();
    }

    if (active === a && r.mediaState === 'OBS_MEDIA_STATE_ERROR') {
      await goLive();
      report(Error('OBS เล่นคลิปไม่ได้'));
    }
  } catch {
  } finally {
    polling = false;
  }
}, 500);

function uploadLogo(b) {
  const m =
    /^data:image\/(png|jpeg|webp);base64,([A-Za-z0-9+/=\r\n]+)$/
      .exec(b.data || '');

  if (!m) throw Error('ใช้รูป PNG, JPG หรือ WebP');

  const bin = Buffer.from(m[2], 'base64');

  if (bin.length > 3 * 1024 * 1024) {
    throw Error('รูปต้องไม่เกิน 3 MB');
  }

  const valid = m[1] === 'png'
    ? bin.subarray(0, 8).equals(
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])
      )
    : m[1] === 'jpeg'
      ? bin[0] === 255 && bin[1] === 216
      : bin.toString('ascii', 0, 4) === 'RIFF' &&
        bin.toString('ascii', 8, 12) === 'WEBP';

  if (!valid) throw Error('ไฟล์ภาพไม่ถูกต้อง');

  const name = crypto.randomUUID() + '.' + m[1];
  fs.writeFileSync(path.join(DIR, 'assets', name), bin);

  const url = '/assets/' + name;

  if (b.team === 'main') state.logo = url;
  else if ([0, 1].includes(Number(b.team))) {
    state.teams[Number(b.team)].logo = url;
  } else {
    throw Error('ตำแหน่งโลโก้ไม่ถูกต้อง');
  }

  changed();
}

async function api(route, b) {
  switch (route) {
    case '/api/action':
      scoreAction(b);
      return {};

    case '/api/logo':
      uploadLogo(b);
      return {};

    case '/api/connect':
      await connect(b);
      return {};

    case '/api/setup':
      if (job || active) {
        throw Error('กลับ LIVE และรองานเดิมจบก่อน');
      }
      await setup();
      return {};

    case '/api/buffer':
      if (!(await call('GetReplayBufferStatus')).outputActive) {
        await call('StartReplayBuffer');
      }
      status.buffer = true;
      broadcast();
      return {};

    case '/api/camera': {
      if (state.cameras.length >= 6) {
        throw Error('ต้นแบบนี้กำหนดไม่เกิน 6 กล้อง');
      }

      const c = {
        id: crypto.randomUUID(),
        name: short(
          b.name || 'CAM ' + (state.cameras.length + 1),
          40
        ),
        stream: crypto.randomBytes(12).toString('hex'),
        password: crypto.randomBytes(12).toString('hex')
      };

      state.cameras.push(c);
      changed();
      return {};
    }

    case '/api/take':
      await takeCamera(b.id);
      return {};

    case '/api/live':
      await goLive();
      return {};

    case '/api/vs':
      await goLive(VS);
      return {};

    case '/api/save':
      return exclusive(async n => ({ clip: await capture(n) }));

    case '/api/quick':
      return exclusive(async n => {
        const c = await capture(n);

        await playClip(c, {
          start: Math.max(0, c.duration - 10),
          end: c.duration,
          speed: 1,
          audio: false
        }, n);

        return { clip: c };
      });

    case '/api/play':
      return exclusive(async n => {
        const c = clips.find(c => c.id === b.id);
        if (!c) throw Error('ไม่พบคลิป');

        await playClip(c, b, n);
        return {};
      });

    case '/api/delete': {
      if (job || active) {
        throw Error('หยุด Replay และรองานเดิมจบก่อนลบ');
      }

      const targets = b.all === true
        ? clips.slice()
        : clips.filter(c => c.id === b.id);

      for (const c of targets) {
        if (fs.existsSync(clipPath(c.id))) {
          fs.unlinkSync(clipPath(c.id));
        }
        clips = clips.filter(x => x.id !== c.id);
      }

      changed();
      return {};
    }

    default:
      throw Error('ไม่พบ API');
  }
}

function byteRange(header, size) {
  if (!header) return null;

  const m = /^bytes=(\d*)-(\d*)$/.exec(header);

  if (!m || (!m[1] && !m[2]) || size <= 0) {
    throw Error('range');
  }

  let start = m[1]
    ? Number(m[1])
    : Math.max(0, size - Number(m[2]));

  let end = m[1]
    ? (m[2] ? Math.min(Number(m[2]), size - 1) : size - 1)
    : size - 1;

  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    start >= size ||
    end < start
  ) {
    throw Error('range');
  }

  return { start, end };
}

function serveFile(req, res, file, type) {
  if (!fs.existsSync(file)) {
    res.writeHead(404);
    return res.end();
  }

  const size = fs.statSync(file).size;
  let range;

  try {
    range = byteRange(req.headers.range, size);
  } catch {
    res.writeHead(416, { 'Content-Range': 'bytes */' + size });
    return res.end();
  }

  const h = {
    'Content-Type': type,
    'Accept-Ranges': 'bytes',
    'Content-Length': range ? range.end - range.start + 1 : size
  };

  if (range) {
    h['Content-Range'] =
      'bytes ' + range.start + '-' + range.end + '/' + size;
  }

  res.writeHead(range ? 206 : 200, h);

  if (req.method === 'HEAD') return res.end();

  const s = fs.createReadStream(file, range || {});

  s.on('error', () => res.destroy());
  res.on('close', () => s.destroy());
  s.pipe(res);
}

function json(res, code, obj) {
  if (res.destroyed || res.writableEnded) return;

  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8'
  });

  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');

  const allowed = ['127.0.0.1:' + PORT, 'localhost:' + PORT];

  if (!allowed.includes(req.headers.host)) {
    return json(res, 403, { error: 'Host not allowed' });
  }

  const u = new URL(req.url, BASE);

  try {
    if (req.method === 'POST') {
      if (
        req.headers.origin &&
        !allowed.map(h => 'http://' + h).includes(req.headers.origin)
      ) {
        return json(res, 403, { error: 'Origin not allowed' });
      }

      if (
        !(req.headers['content-type'] || '')
          .startsWith('application/json')
      ) {
        return json(res, 415, { error: 'JSON required' });
      }

      const chunks = [];
      let len = 0;

      for await (const chunk of req) {
        len += chunk.length;

        if (len > 5 * 1024 * 1024) {
          return json(res, 413, { error: 'ข้อมูลใหญ่เกินไป' });
        }

        chunks.push(chunk);
      }

      const b = JSON.parse(
        Buffer.concat(chunks).toString('utf8') || '{}'
      );

      return json(res, 200, {
        ok: true,
        ...(await api(u.pathname, b))
      });
    }

    if (!['GET', 'HEAD'].includes(req.method)) {
      return json(res, 405, { error: 'Method not allowed' });
    }

    if (u.pathname === '/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive'
      });

      res.write('data: ' + JSON.stringify(payload()) + '\n\n');
      peers.add(res);
      res.on('close', () => peers.delete(res));
      return;
    }

    if (u.pathname === '/api/state') {
      return json(res, 200, payload());
    }

    if (u.pathname === '/api/cameras') {
      const cameras = await Promise.all(
        state.cameras.map(async c => {
          const links = cameraURLs(c);

          return {
            id: c.id,
            name: c.name,
            ...links,
            qr: await QR.toDataURL(links.push, { width: 220 })
          };
        })
      );

      return json(res, 200, { cameras });
    }

    if (/^\/assets\/[a-f0-9-]+\.(png|jpeg|webp)$/.test(u.pathname)) {
      return serveFile(
        req,
        res,
        path.join(DIR, u.pathname.slice(1)),
        'image/' + u.pathname.split('.').pop()
      );
    }

    const m = /^\/media\/([a-f0-9-]+)\.mp4$/.exec(u.pathname);

    if (m && clips.some(c => c.id === m[1])) {
      return serveFile(req, res, clipPath(m[1]), 'video/mp4');
    }

    if (u.pathname === '/') {
      return serveFile(
        req,
        res,
        path.join(__dirname, 'app.html'),
        'text/html; charset=utf-8'
      );
    }

    return json(res, 404, { error: 'Not found' });
  } catch (e) {
    json(res, 400, { error: e.message || String(e) });
  }
});

setInterval(() => {
  let dirty = false;

  for (const s of Object.values(state.sports)) {
    for (const key of ['clock', 'shot']) {
      if (
        s[key].running &&
        s[key].down &&
        clockValue(s[key]) === 0
      ) {
        pause(s[key]);
        if (key === 'clock') pause(s.shot);
        dirty = true;
      }
    }
  }

  if (state.event && Date.now() >= state.event.until) {
    state.event = null;
    dirty = true;
  }

  if (dirty) changed();
}, 100);

setInterval(() => {
  for (const p of peers) {
    p.write('event: tick\ndata: ' + Date.now() + '\n\n');
  }
}, 2000);

setInterval(checkpoint, 5000);

process.on('SIGINT', () => {
  for (const s of Object.values(state.sports)) {
    pause(s.clock);
    pause(s.shot);
  }

  checkpoint();
  if (child) child.kill();
  process.exit();
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(
    '\nSPORTS CONTROL V1\nOpen ' + BASE + '\nCtrl+C to stop.\n'
  );
});