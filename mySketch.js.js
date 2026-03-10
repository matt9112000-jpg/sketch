/***** ===== 基本常數 ===== *****/
const ASPECT_W = 4, ASPECT_H = 5;
const BORDER_THICK = 12, BORDER_HALF = BORDER_THICK/2;

let cols = 6, rows = 8;
let blockSize, innerW, innerH;
let board, currentPiece;
let emptyCells = cols * rows;
let dropCounter = 0, dropInterval = 800, lastTime = 0;

let canvasX = 0, canvasY = 0;
let mainCanvas = null;

/***** 玩家 / 狀態 *****/
let playerName = '';
let inputComplete = false;
let nameInput;

let endBlocks = 0;
let gameState = 'input'; // 'input','playing','endedWait','gameover','rewardPreview','leaderboard'
let NAME_INPUT_W = 232;
let NAME_INPUT_H = 36;

/***** 本地備援排行榜 *****/
const STORAGE_KEY = 'tetris_scores';
const CLOUD_CACHE_KEY = 'tetris_scores_cache';
const MODEL_URL = './cartridge.glb';
const MODEL_STICKER_URL = './cartridge_sticker.glb';
const MODEL_CASE_URL = './box_case.glb';
const MODEL_PANEL_URL = './box_panel.glb';
const MODEL_PART3_URL = './box_part3.glb';
const CUBE_MODEL_URL = './cube.glb';
const PREVIEW_MODE = new URLSearchParams(window.location.search).has('preview');
/***** Three.js（非 ESM 版） *****/
const THREE_CDNS = [
  'https://cdn.jsdelivr.net/npm/three@0.128.0/build/three.min.js',
];
const GLTF_CDNS = [
  'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/loaders/GLTFLoader.js',
];
const ORBIT_CDNS = [
  'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js',
];

function loadScript(url){
  return new Promise((resolve, reject)=>{
    const s = document.createElement('script');
    s.src = url; s.async = true; s.crossOrigin = 'anonymous';
    s.onload = ()=>resolve(url);
    s.onerror = ()=>reject(new Error('load fail: '+url));
    document.head.appendChild(s);
  });
}
async function loadOneFrom(list){
  let lastErr;
  for (const url of list){
    try { return await loadScript(url); }
    catch(e){ lastErr = e; console.warn(e.message); }
  }
  throw lastErr;
}
async function ensureThreeScripts(){
  if (!window.THREE) { await loadOneFrom(THREE_CDNS); }
  if (!THREE.GLTFLoader) { await loadOneFrom(GLTF_CDNS); }
  if (!THREE.OrbitControls) { await loadOneFrom(ORBIT_CDNS); }
  if (!window.THREE || !THREE.GLTFLoader || !THREE.OrbitControls){
    throw new Error('Three.js / GLTFLoader / OrbitControls not available');
  }
}

/***** 形狀 / 顏色 *****/
const SHAPES = {
  single: [[1]],
  line2: [[1,1]],
  line3: [[1,1,1]],
  square2: [[1,1],[1,1]],
  L3: [[1,0],[1,0],[1,1]],
  T3: [[1,1,1],[0,1,0]],
  S3: [[0,1,1],[1,1,0]]
};
const PALETTE = ['#FCC730','#1A26FF','#FF4622','#FF3BDA','#6DFF69','#FF99B1','#4C4C4C'];
const BG_BLUE = '#000E51';
const PINK = '#EE00B8';

/***** 字型 *****/
let FONT_FAMILY = 'Montserrat';
let FONT_READY = false;

/***** Firestore（雲端排行榜） *****/
let FIREBASE_ENABLED = true;
let db = null;
let topScores = [];
const FIREBASE_CONFIG = {
  apiKey: "AIzaSyDwhrpuRxLdo3_nq_W7TKn4JKItyX7WyCQ",
  authDomain: "insufficientintro.firebaseapp.com",
  projectId: "insufficientintro",
};

/***** 手機提示按鈕 *****/
const BTN_MARGIN = 8;
const BTN_SIZE_MULT = 0.8;
let UI_BTN = { left:{x:0,y:0,s:0}, down:{x:0,y:0,s:0}, right:{x:0,y:0,s:0}, rotate:{x:0,y:0,s:0} };

/***** Leaderboard 視覺 *****/
const SHOW_CLEAR = true;
let LB_THUMB_RATIO = 0.30;
let LB_SIZES = [28,20,20];
let BOUNCE_OFFSET = 16;
let SCALE_HOVER = 1.15;
let lbHover = -1, lbActive = -1, lbActiveUntil = 0, lbRects = [];

/***** RWD *****/
let IS_MOBILE = false;
let BTN_PAD_SMALL='8px 12px', BTN_PAD_LARGE='10px 18px', BTN_FZ_SMALL='14px', BTN_FZ_LARGE='18px';

/***** 最近一局快照（儲存 PNG 用） *****/
let lastSnapshot = null, lastName = '', lastBlocks = 0;

/***** input 背景掉落動畫 *****/
let introPieces = [];
let introLastTime = 0, introSpawnTimer = 0, introSpawnEvery = 650;

/***** 跑馬燈（行動版） *****/
let mqTop = null, mqBottom = null;
let pendingLeaderboardAfterReward = false;
let charmCloseHook = null;

/***** 遊玩次數（右下角顯示） *****/
const PLAYED_KEY = 'tetris_played_count';
let playedCount = 0;
let cloudPlayedCount = null;
let lastActivityState = '';
let lastActivityAt = 0;
let lastProgressPublishAt = 0;
function loadPlayed(){ playedCount = parseInt(localStorage.getItem(PLAYED_KEY) || '0'); }
function incPlayed(){ playedCount++; localStorage.setItem(PLAYED_KEY, String(playedCount)); }
function getDisplayedPlayedCount(){
  if (typeof cloudPlayedCount === 'number' && cloudPlayedCount >= 0) return cloudPlayedCount;
  return playedCount;
}
async function incPlayedCloud(){
  if (!db || !firebase || !firebase.firestore || !firebase.firestore.FieldValue) return;
  try{
    await db.collection('gameStats').doc('global').set({
      playedCount: firebase.firestore.FieldValue.increment(1),
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge:true });
  }catch(e){ console.warn('incPlayedCloud failed', e); }
}
function subscribePlayedCount(){
  if (!db) return;
  db.collection('gameStats').doc('global').onSnapshot((doc)=>{
    const data = doc && doc.data ? doc.data() : null;
    if (data && typeof data.playedCount === 'number') cloudPlayedCount = data.playedCount;
  }, (err)=>console.warn('playedCount subscribe error', err));
}
async function publishActivity(state, extra = {}){
  if (!db || PREVIEW_MODE) return;
  const now = millis ? millis() : Date.now();
  if (state === lastActivityState && (now - lastActivityAt) < 700) return;
  lastActivityState = state;
  lastActivityAt = now;
  const safeName = (playerName || 'Guest').slice(0,20);
  try{
    await db.collection('activity').doc(nameKeyFrom(safeName)).set({
      name: safeName,
      nameKey: nameKeyFrom(safeName),
      state,
      score: typeof extra.score === 'number' ? extra.score : null,
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge:true });
  }catch(e){ console.warn('publishActivity failed', e); }
}

/***** ====== 小工具 ====== *****/
function stylePill(btn, base="#FF5722", hover="#FF784E"){
  btn.style('background','rgba(0,0,0,.30)').style('color','#ffffff')
     .style('border','1px solid rgba(255,255,255,.14)').style('border-radius','12px')
     .style('padding','10px 16px').style('font-weight','800').style('cursor','pointer')
     .style('backdrop-filter','blur(8px)').style('-webkit-backdrop-filter','blur(8px)');
  btn.mouseOver(()=>{ btn.style('border-color','rgba(255,255,255,.28)'); });
  btn.mouseOut(()=>{ btn.style('border-color','rgba(255,255,255,.14)'); });
}
function createBoard(){
  emptyCells = rows * cols;
  return Array.from({ length: rows }, () => Array(cols).fill(null));
}
function isValid(p, dx, dy, mat = p.matrix) {
  if (!p || !mat) return false;
  for (let y=0; y<mat.length; y++){
    for (let x=0; x<mat[y].length; x++){
      if (mat[y][x]){
        const nx = p.x + x + dx, ny = p.y + y + dy;
        if (nx < 0 || nx >= cols || ny < 0 || ny >= rows) return false;
        if (board[ny][nx] !== null) return false;
      }
    }
  }
  return true;
}
function rotateMatrix(mat){ return mat[0].map((_,i)=>mat.map(r=>r[i])).reverse(); }
function colFromIndex(i){ return color(PALETTE[i]); }
function drawCell(px, py, size, fillCol, strokeCol=255){
  noStroke(); fill(fillCol); rect(px, py, size, size);
  stroke(strokeCol); strokeWeight(1); noFill(); rect(px, py, size, size);
}
function nameKeyFrom(name){
  const k = (name||'').trim().toLowerCase().replace(/[^a-z0-9_]/g,'_').slice(0,20);
  return k || 'player';
}
function encodeBoardSnapshot(){
  let out=''; for(let y=0;y<rows;y++){ for(let x=0;x<cols;x++){ const v=board[y][x]; out += (v===null?'8':String(v)); } }
  return out;
}
function drawSnapshot(snapshot, x, y, cell){
  if(!snapshot || snapshot.length !== cols*rows) return;
  push(); noStroke();
  for(let i=0;i<snapshot.length;i++){
    const ch=snapshot[i]; if(ch!=='8'){
      const idx = ch.charCodeAt(0)-48, cx=i%cols, cy=Math.floor(i/cols);
      fill(colFromIndex(idx)); rect(x+cx*cell,y+cy*cell,cell,cell);
      noFill(); stroke(255); strokeWeight(1); rect(x+cx*cell,y+cy*cell,cell,cell); noStroke();
    }
  }
  pop();
}
function drawSnapshotToGraphics(g, snapshot, x, y, cell){
  if(!snapshot || snapshot.length !== cols*rows) return;
  g.push(); g.noStroke();
  for(let i=0;i<snapshot.length;i++){
    const ch=snapshot[i]; if(ch!=='8'){
      const idx = ch.charCodeAt(0)-48, cx=i%cols, cy=Math.floor(i/cols);
      g.fill(colFromIndex(idx)); g.rect(x+cx*cell,y+cy*cell,cell,cell);
      g.noFill(); g.stroke(255); g.strokeWeight(1); g.rect(x+cx*cell,y+cy*cell,cell,cell); g.noStroke();
    }
  }
  g.pop();
}
function saveLastGamePng(){
  const snap = lastSnapshot || encodeBoardSnapshot();
  const nm = (lastName || playerName || 'player').replace(/ /g,'_');
  const pg = createGraphics(width, height);
  pg.textFont('Montserrat'); pg.textStyle(BOLD);
  pg.background('#ffffff');
  pg.noStroke(); pg.fill(BG_BLUE); pg.rect(BORDER_HALF, BORDER_HALF, innerW, innerH);
  pg.stroke(PINK); pg.strokeWeight(BORDER_THICK); pg.noFill(); pg.rect(0,0,width,height);
  pg.noStroke(); pg.fill('#FF3BDA'); pg.textAlign(pg.LEFT, pg.TOP);
  pg.textSize(Math.max(14, blockSize*0.35)); pg.text(nm, BORDER_HALF+6, BORDER_HALF+4);
  drawSnapshotToGraphics(pg, snap, BORDER_HALF, BORDER_HALF, blockSize);
  saveCanvas(pg.canvas, `${nm}_${lastBlocks}`, 'png');
}
function lockPiece(){
  const m=currentPiece.matrix;
  for (let y=0;y<m.length;y++){
    for (let x=0;x<m[y].length;x++){
      if (!m[y][x]) continue;
      const by = currentPiece.y + y;
      const bx = currentPiece.x + x;
      if (board[by][bx] === null) emptyCells--;
      board[by][bx] = currentPiece.cidx;
    }
  }
}

/***** 本地備援排行榜（同名只留最佳） *****/
function updateLocalTopScores(name, score, snapshot) {
  let list = JSON.parse(localStorage.getItem(STORAGE_KEY)||'[]');
  const k = nameKeyFrom(name);
  const i = list.findIndex(r=> (r.name && nameKeyFrom(r.name)===k) || r.nameKey===k );
  if (i>=0) { if (score < list[i].score) { list[i].score = score; list[i].snapshot = snapshot||null; } }
  else { list.push({name, nameKey:k, score, snapshot:snapshot||null}); }
  list.sort((a,b)=>a.score-b.score); list=list.slice(0,3);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
  if (!db) topScores = list;
}
async function saveScore(name, score, snapshot) {
  lastSnapshot = snapshot; lastName = name; lastBlocks = score;
  updateLocalTopScores(name, score, snapshot);
  if (!FIREBASE_ENABLED || !db) return;
  try{
    const ref = db.collection('scores').doc(nameKeyFrom(name));
    await ref.set({ name: name.slice(0,20), nameKey: nameKeyFrom(name), score, snapshot: snapshot||null,
                    createdAt: firebase.firestore.FieldValue.serverTimestamp() }, {merge:true});
  }catch(e){ console.log('cloud save failed', e); }
}
function getTopScoresLocal(){
  let list = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
  list.sort((a,b)=>a.score-b.score); return list.slice(0,3);
}

/***** Firebase 啟動 & 訂閱 *****/
function initFirebase(){
  try{
    if (!FIREBASE_ENABLED) return;
    if (typeof firebase === 'undefined') { FIREBASE_ENABLED=false; return; }
    if (!firebase.apps.length) firebase.initializeApp(FIREBASE_CONFIG);
    db = firebase.firestore(); firebase.firestore().enablePersistence().catch(()=>{});
    subscribeLeaderboard();
    subscribePlayedCount();
  }catch(e){ FIREBASE_ENABLED=false; console.warn('Firebase init failed:', e); }
}
function subscribeLeaderboard(){
  if (!db) return;
  db.collection('scores').orderBy('score','asc').orderBy('createdAt','asc').limit(50)
    .onSnapshot(snap=>{
      const arr=[]; snap.forEach(d=>arr.push(d.data()));
      const best=new Map();
      for (const r of arr){
        const k=r.nameKey||nameKeyFrom(r.name||'');
        if(!best.has(k) || r.score < best.get(k).score) best.set(k, r);
      }
      topScores = [...best.values()].sort((a,b)=>a.score-b.score).slice(0,3);
      saveCloudCache(topScores);
    }, err=>console.log('onSnapshot error', err));
}

/***** 清除排行榜（local + cloud） *****/
async function clearCloudScores(){
  if (!db) return; const snap = await db.collection('scores').get();
  const batch = db.batch(); snap.forEach(doc=>batch.delete(doc.ref)); await batch.commit();
}
function clearLocalScores(){
  localStorage.removeItem(STORAGE_KEY);
  localStorage.removeItem(CLOUD_CACHE_KEY);
}
async function clearScores(){
  const want = confirm('Clear leaderboard?\n（本地與雲端都會嘗試清除）');
  if (!want) return;
  try{ await clearCloudScores(); }catch(e){ console.warn('清雲端失敗（可能是權限）', e); }
  try{ clearLocalScores(); }catch(e){}
  topScores = []; renderLeaderboard();
}

/***** 雲端榜單快取 & Durable Storage *****/
async function ensureDurableStorage(){
  if (navigator.storage && navigator.storage.persist) {
    try {
      const ok = await navigator.storage.persist();
      console.log('Durable storage:', ok ? 'granted' : 'not granted');
    } catch(e){ console.warn('persist() failed', e); }
  }
}
function saveCloudCache(list){
  try { localStorage.setItem(CLOUD_CACHE_KEY, JSON.stringify(list || [])); } catch(e){}
}
function getCloudCache(){
  try { return JSON.parse(localStorage.getItem(CLOUD_CACHE_KEY) || '[]'); } catch(e){ return []; }
}
function hydrateScoresEarly(){
  const cache = getCloudCache();
  topScores = (cache && cache.length) ? cache : getTopScoresLocal();
}

/***** p5 lifecycle *****/
function setup(){
  ensureDurableStorage();
  hydrateScoresEarly();

  if (!select('#mqStyle')){
    const css = `
      @keyframes scrollX { from { transform: translate3d(0,0,0); } to { transform: translate3d(-50%,0,0);} }
      .mq { position:absolute; left:0; width:100vw; overflow:hidden; pointer-events:none; z-index:2; color:${PINK}; background:#fff;
        font-weight:800; font-family: Montserrat, system-ui, -apple-system, Roboto, Arial, sans-serif; }
      .mq .track { display:inline-flex; will-change: transform; animation: scrollX 35s linear infinite; backface-visibility:hidden; transform: translateZ(0); }
      .mq.bottom .track { animation-direction: reverse; }
      .mq .content { display:inline-block; white-space:nowrap; padding-right: 40px; }
    `;
    const st = createElement('style', css); st.id('mqStyle'); st.parent(document.head);
  }

  const gf = createElement('link');
  gf.attribute('rel','stylesheet');
  gf.attribute('href','https://fonts.googleapis.com/css2?family=Montserrat:wght@600;700;800&display=swap');
  gf.parent(document.head);
  if (document.fonts && document.fonts.load) {
    document.fonts.load('800 24px ' + FONT_FAMILY).then(()=>{ FONT_READY = true; textFont(FONT_FAMILY); });
  } else { textFont(FONT_FAMILY); }

  calculateLayout(); frameRate(60); initFirebase();
  board = createBoard(); loadPlayed();

  nameInput = createInput();
  nameInput.attribute('placeholder','Enter your IG')
           .attribute('inputmode','text').attribute('autocomplete','off')
           .attribute('autocorrect','off').attribute('autocapitalize','off');
  nameInput.style('position','absolute').style('z-index','10010').style('pointer-events','auto')
           .style('font-weight','600')
           .style('font-size', IS_MOBILE ? '17px' : '18px')
           .style('color','#f4f6ff')
           .style('background','rgba(8,14,66,0.86)')
           .style('border','1px solid rgba(255,59,218,0.62)')
           .style('border-radius','10px')
           .style('padding','0 10px')
           .style('outline','none')
           .style('box-shadow','0 0 0 1px rgba(255,255,255,0.08) inset, 0 0 16px rgba(255,59,218,0.22)')
           .style('font-family', "Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif")
           .size(NAME_INPUT_W, NAME_INPUT_H);
  centerInput(); nameInput.elt.focus();
  nameInput.elt.addEventListener('keydown', e=>{
    if (e.key === 'Enter' && nameInput.value().trim()){
      playerName = nameInput.value().trim().slice(0,20);
      inputComplete = true; gameState = 'playing'; nameInput.hide(); spawnPiece();
      lastProgressPublishAt = 0;
      publishActivity('playing');
    }
  });
}
function windowResized(){ calculateLayout(); if(!inputComplete) centerInput(); positionMarquees(); clearButtons(); }
function centerInput(){
  const w = innerW + BORDER_THICK, h = innerH + BORDER_THICK;
  nameInput.size(NAME_INPUT_W, NAME_INPUT_H);
  nameInput.position(canvasX + (w - NAME_INPUT_W)/2, canvasY + (h - NAME_INPUT_H)/2);
}
function applyResponsiveUI(){
  IS_MOBILE =
    (windowWidth <= 700) ||
    (/Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent));

  if (IS_MOBILE){
    // 行動版
    LB_THUMB_RATIO = 0.25;
    LB_SIZES = [24, 18, 18];

    BTN_PAD_SMALL = '6px 10px';
    BTN_PAD_LARGE = '8px 14px';
    BTN_FZ_SMALL  = '12px';
    BTN_FZ_LARGE  = '15px';
    NAME_INPUT_W = 248;
    NAME_INPUT_H = 40;
  } else {
    // 桌面版
    LB_THUMB_RATIO = 0.30;
    LB_SIZES = [28, 20, 20];

    BTN_PAD_SMALL = '8px 12px';
    BTN_PAD_LARGE = '10px 18px';
    BTN_FZ_SMALL  = '14px';
    BTN_FZ_LARGE  = '18px';
    NAME_INPUT_W = 280;
    NAME_INPUT_H = 42;
  }
}

function calculateLayout(){
  let cw = windowWidth - BORDER_THICK, ch = windowHeight - BORDER_THICK;
  if (cw * ASPECT_H > ch * ASPECT_W) cw = ch * ASPECT_W / ASPECT_H; else ch = cw * ASPECT_H / ASPECT_W;
  blockSize = floor(min(cw/cols, ch/rows)); innerW = cols * blockSize; innerH = rows * blockSize;
  const w = innerW + BORDER_THICK, h = innerH + BORDER_THICK;
  if (!mainCanvas){
    mainCanvas = createCanvas(w, h);
    mainCanvas.style('z-index','1');
  } else {
    resizeCanvas(w, h);
  }
  canvasX = (windowWidth - w) / 2; canvasY = (windowHeight - h) / 2;
  mainCanvas.position(canvasX, canvasY);

  const s = min(blockSize * BTN_SIZE_MULT, innerW/6); const yPos = BORDER_HALF + innerH - s - BTN_MARGIN;
  UI_BTN.left   = { x: BORDER_HALF + BTN_MARGIN,              y: yPos,                  s: s };
  UI_BTN.down   = { x: BORDER_HALF + innerW/2 - s/2,          y: yPos,                  s: s };
  UI_BTN.right  = { x: BORDER_HALF + innerW - s - BTN_MARGIN, y: yPos,                  s: s };
  UI_BTN.rotate = { x: BORDER_HALF + innerW/2 - s/2,          y: yPos - s - BTN_MARGIN, s: s };

  select('body').style('background','#ffffff'); applyResponsiveUI();
  ensureMarquees(); positionMarquees();
  introPieces = []; introLastTime = 0; introSpawnTimer = 0;
}
function draw(){
  background('#ffffff');
  noStroke(); fill(BG_BLUE); rect(BORDER_HALF, BORDER_HALF, innerW, innerH);
  stroke(PINK); strokeWeight(BORDER_THICK); noFill(); rect(0,0,width,height);

  if (gameState === 'input'){
    push(); translate(BORDER_HALF, BORDER_HALF); updateIntroPieces(); drawIntroPieces(); pop();
    if (!PREVIEW_MODE){
      const promptW = max(280, NAME_INPUT_W + 28);
      noStroke();
      fill(7, 13, 58, 210);
      rect(width/2 - promptW/2, height/2 - 54, promptW, 30, 9);
      stroke(255, 59, 218, 170);
      strokeWeight(1.2);
      noFill();
      rect(width/2 - promptW/2, height/2 - 54, promptW, 30, 9);
      noStroke();
      fill('#ffd6fa');
      textAlign(CENTER,CENTER);
      textSize(12);
      textStyle(BOLD);
      text('ENTER YOUR IG TO START', width/2, height/2 - 39);
      nameInput.show(); nameInput.elt.focus();
    } else {
      nameInput.hide();
    }
    return;
  }
  if (gameState === 'playing'){
    const safePlayerName = fitTextToWidth(playerName, innerW - 12);
    const padX = 8;
    const labelW = max(88, min(innerW - 12, safePlayerName.length * max(9, blockSize * 0.27) + 24));
    noStroke();
    fill(10, 16, 74, 215);
    rect(BORDER_HALF + 6, BORDER_HALF + 4, labelW, 26, 8);
    stroke(255, 59, 218, 180);
    strokeWeight(1.1);
    noFill();
    rect(BORDER_HALF + 6, BORDER_HALF + 4, labelW, 26, 8);
    noStroke();
    fill('#ffd4f8');
    textSize(max(13, blockSize*0.33));
    textAlign(LEFT,CENTER);
    textStyle(BOLD);
    text(safePlayerName, BORDER_HALF + 6 + padX, BORDER_HALF + 17);
    handleDrop();
    if (!PREVIEW_MODE){
      const now = millis ? millis() : Date.now();
      if (now - lastProgressPublishAt > 3200){
        lastProgressPublishAt = now;
        publishActivity('playing', { score: emptyCells });
      }
    }
    push(); translate(BORDER_HALF, BORDER_HALF); drawBoard(); drawPiece(); pop();
    if (!PREVIEW_MODE) drawHintSquares();
    return;
  }
  if (gameState === 'endedWait'){
    push(); translate(BORDER_HALF, BORDER_HALF); drawBoard(); drawPiece(); pop();
    if (!select('#nextPromptBtn')){
      clearButtons();
      createStyledButton('nextPromptBtn','Next', canvasX + width/2 - 50, canvasY + height/2 - 14,
        () => { gameState = 'gameover'; clearButtons(); });
    }
    return;
  }
  if (gameState === 'gameover'){
    endBlocks = emptyCells;
    if (!select('#savedFlag')){
      const snap = encodeBoardSnapshot();
      lastSnapshot = snap; lastName = playerName; lastBlocks = endBlocks;
      saveScore(playerName, endBlocks, snap); incPlayed(); incPlayedCloud(); publishActivity('gameover', { score:endBlocks });
      const flag = createDiv(''); flag.id('savedFlag'); flag.style('display','none');
    }
    noStroke(); fill('#FF3BDA'); textAlign(CENTER,CENTER); textStyle(BOLD);
    textSize(28); text('Game Over!', width/2, height/2 - 26);
    textSize(22); text(`Empty Blocks: ${endBlocks}`, width/2, height/2 + 6);
    if (!select('#nextBtn')){
      clearButtons();
      createStyledButton('nextBtn','Next',
        canvasX + width/2 - 50, canvasY + height/2 + 40,
        () => {
          clearButtons();
          removeSavedFlag();
          pendingLeaderboardAfterReward = true;
          gameState = 'rewardPreview';
          openCharmPreview3D({
            fromGameOver: true,
            onClose: () => {
              if (pendingLeaderboardAfterReward){
                pendingLeaderboardAfterReward = false;
                gameState = 'leaderboard';
                publishActivity('leaderboard', { score:endBlocks });
                clearButtons();
                removeSavedFlag();
              }
            }
          });
        });
    }
    return;
  }
  if (gameState === 'rewardPreview'){
    // Keep a clean pause state while reward popup is open.
    background('#ffffff');
    noStroke(); fill(BG_BLUE); rect(BORDER_HALF, BORDER_HALF, innerW, innerH);
    stroke(PINK); strokeWeight(BORDER_THICK); noFill(); rect(0,0,width,height);
    return;
  }
  if (gameState === 'leaderboard'){
    renderLeaderboard(); return;
  }
}

function drawPlayedBadge(){
  const count = getDisplayedPlayedCount();
  const label = 'GLOBAL PLAYS';
  const num = String(count);
  const padX = 10;
  const padY = 6;
  const numW = max(38, num.length * 8 + 10);
  const w = 108 + numW;
  const h = 28;
  const x = width - BORDER_HALF - w - 6;
  const y = BORDER_HALF + innerH - h - 6;

  noStroke();
  fill(14, 22, 92, 230);
  rect(x, y, w, h, 10);
  stroke(255, 59, 218, 210);
  strokeWeight(1.4);
  noFill();
  rect(x, y, w, h, 10);

  noStroke();
  fill('#ffd3f8');
  textAlign(LEFT, CENTER);
  textStyle(BOLD);
  textSize(10);
  text(label, x + padX, y + h / 2 + 0.5);

  fill('#ff3bda');
  rect(x + w - numW - 4, y + 4, numW, h - 8, 7);
  fill('#ffffff');
  textAlign(CENTER, CENTER);
  textSize(13);
  text(num, x + w - numW / 2 - 4, y + h / 2 + 0.5);
}

/***** 遊戲流程 *****/
function handleDrop(){ const now=millis(), d=now-lastTime; lastTime=now; dropCounter+=d; if (dropCounter>dropInterval){ moveDown(); dropCounter=0; } }
function moveDown(){
  if (!currentPiece) return;
  if (isValid(currentPiece,0,1)) { currentPiece.y++; return; }
  lockPiece(); if (currentPiece.y===0){ endGame(); return; } spawnPiece();
}
function endGame(){ gameState='endedWait'; }
function spawnPiece(){
  const keys = Object.keys(SHAPES); const k = random(keys);
  currentPiece = { matrix: SHAPES[k].map(r=>r.slice()), x: floor(cols/2)-floor(SHAPES[k][0].length/2), y:0, cidx: floor(random(PALETTE.length)) };
  if (!isValid(currentPiece,0,0)) endGame();
}

/***** 輸入 *****/
let touchStartTime = 0;
function pointInRect(px, py, r){ return px >= r.x && px <= r.x + r.s && py >= r.y && py <= r.y + r.s; }
function touchStarted(){
  if (PREVIEW_MODE) return true;
  if (gameState === 'input') return true;
  if (gameState === 'leaderboard'){
    if (touches.length){ const t=touches[0]; updateLbHover(t.x,t.y); if (lbHover>=0){ lbActive=lbHover; lbActiveUntil=millis()+250; } }
    touchStartTime = millis(); return true;
  }
  if (gameState === 'playing'){
    if (touches.length){
      const t=touches[0];
      if (pointInRect(t.x,t.y,UI_BTN.left))   { if (isValid(currentPiece,-1,0)) currentPiece.x--; return false; }
      if (pointInRect(t.x,t.y,UI_BTN.down))   { moveDown(); return false; }
      if (pointInRect(t.x,t.y,UI_BTN.right))  { if (isValid(currentPiece, 1,0)) currentPiece.x++; return false; }
      if (pointInRect(t.x,t.y,UI_BTN.rotate)) { const r=rotateMatrix(currentPiece.matrix); if (isValid(currentPiece,0,0,r)) currentPiece.matrix=r; return false; }
    }
    return true;
  }
  touchStartTime = millis(); return true;
}
function touchEnded(){ return true; }
function keyPressed(){
  if (PREVIEW_MODE) return;
  if (gameState === 'leaderboard') return;
  if (!inputComplete || gameState!=='playing') return;
  if (keyCode===LEFT_ARROW && isValid(currentPiece,-1,0)) currentPiece.x--;
  else if (keyCode===RIGHT_ARROW && isValid(currentPiece,1,0)) currentPiece.x++;
  else if (keyCode===DOWN_ARROW) moveDown();
  else if (keyCode===UP_ARROW){ const r=rotateMatrix(currentPiece.matrix); if (isValid(currentPiece,0,0,r)) currentPiece.matrix=r; }
}

/***** 繪製 *****/
function drawBoard(){
  for (let y=0;y<rows;y++){ for(let x=0;x<cols;x++){ const v=board[y][x]; if (v!==null){ const px=x*blockSize, py=y*blockSize; drawCell(px,py,blockSize,colFromIndex(v),255); } } }
}
function drawPiece(){
  if (!currentPiece) return;
  const m=currentPiece.matrix;
  for (let y=0;y<m.length;y++){ for(let x=0;x<m[y].length;x++){ if(m[y][x]){ const px=(currentPiece.x+x)*blockSize, py=(currentPiece.y+y)*blockSize; drawCell(px,py,blockSize,colFromIndex(currentPiece.cidx),255); } } }
}
function drawHintSquares(){
  const spec = [
    { b:UI_BTN.left,   key:'\u25C0', color:'#b8f0ff' },
    { b:UI_BTN.down,   key:'\u25BC', color:'#ffe27a' },
    { b:UI_BTN.right,  key:'\u25B6', color:'#bfffc5' },
    { b:UI_BTN.rotate, key:'\u21BB', color:'#ffc0f2' }
  ];
  for (const it of spec){
    const b = it.b;
    noStroke();
    fill(0, 0, 0, 90);
    rect(b.x + 2, b.y + 2, b.s, b.s, 8);
    fill(24, 30, 52, 205);
    rect(b.x, b.y, b.s, b.s, 8);
    noFill();
    stroke(it.color);
    strokeWeight(1.6);
    rect(b.x, b.y, b.s, b.s, 8);
    noStroke();
    fill(it.color);
    textAlign(CENTER, CENTER);
    textStyle(BOLD);
    textSize(max(15, b.s * 0.4));
    text(it.key, b.x + b.s/2, b.y + b.s*0.54);
  }
}

/***** Intro 背景掉落 *****/
function spawnIntroPiece(){
  const keys=Object.keys(SHAPES), k=random(keys), mat=SHAPES[k], w=mat[0].length, h=mat.length;
  const x=floor(random(0,max(1,cols-w+1))), cidx=floor(random(PALETTE.length)), speed=random(0.8,1.7);
  introPieces.push({ matrix:mat, x, y:-h, cidx, speed });
}
function updateIntroPieces(){
  const now=millis(); if (introLastTime===0) introLastTime=now;
  const dt=(now-introLastTime)/1000; introLastTime=now;
  introSpawnTimer += dt*1000; if (introSpawnTimer >= introSpawnEvery){ introSpawnTimer = 0; spawnIntroPiece(); }
  for (const p of introPieces){ p.y += p.speed * dt; }
  introPieces = introPieces.filter(p => p.y < rows + 1);
}
function drawIntroPieces(){
  for (const p of introPieces){
    const m=p.matrix;
    for (let y=0;y<m.length;y++){
      for (let x=0;x<m[y].length;x++){
        if (m[y][x]){ const px=(p.x+x)*blockSize, py=(p.y+y)*blockSize; const fillCol=colFromIndex(p.cidx); fillCol.setAlpha(150); drawCell(px,py,blockSize,fillCol,255); }
      }
    }
  }
}

/***** Leaderboard *****/
function renderLeaderboard(){
  const isCompactLb = IS_MOBILE || innerW < 380;
  background('#ffffff');
  noStroke(); fill(BG_BLUE); rect(BORDER_HALF, BORDER_HALF, innerW, innerH);
  for (let i=0; i<8; i++){
    const a = map(i, 0, 7, 16, 52);
    fill(255, 59, 218, a);
    rect(BORDER_HALF + i * (innerW/8), BORDER_HALF, innerW/16, innerH);
  }
  stroke(PINK); strokeWeight(BORDER_THICK); noFill(); rect(0,0,width,height);
  noStroke();
  fill('#f9f7ff');
  textAlign(CENTER, TOP);
  textStyle(BOLD);
  const titleY = BORDER_HALF + (isCompactLb ? 6 : 8);
  const titleSize = isCompactLb ? max(16, innerW * 0.05) : max(20, innerW * 0.062);
  textSize(titleSize);
  text('LEADERBOARD', width/2, titleY);
  fill('#ffd9f8');
  textSize(isCompactLb ? max(10, innerW * 0.022) : max(12, innerW * 0.025));
  text('Top survivors in insufficient space', width/2, titleY + titleSize + (isCompactLb ? 12 : 14));
  drawPlayedBadge();

  const btnBaseY = isCompactLb ? 16 : 20;
  if (SHOW_CLEAR && !select('#clearBtn')) createStyledButton('clearBtn','Clear', canvasX + 12, canvasY + btnBaseY, clearScores);
  if (!select('#saveBtn')) createStyledButton('saveBtn','Save', canvasX + 12, canvasY + btnBaseY + 36, saveLastGamePng);
  if (!select('#makeCharmBtnLB')) createStyledButton('makeCharmBtnLB','★ Make a Charm', canvasX + 12, canvasY + btnBaseY + 72, () => { openCharmPreview3D(); });

  let source = [];
  if (topScores && topScores.length) source = topScores;
  else {
    const cache = getCloudCache();
    source = (cache && cache.length) ? cache : getTopScoresLocal();
  }

  lbRects = [];
  const podiumY = BORDER_HALF + innerH * (isCompactLb ? 0.77 : 0.74);
  const cx = BORDER_HALF + innerW/2;
  const lane = innerW * (isCompactLb ? 0.33 : 0.31);
  const colW = innerW * (isCompactLb ? 0.18 : 0.20);
  const colHeights = isCompactLb ? [innerH * 0.30, innerH * 0.20, innerH * 0.16] : [innerH * 0.33, innerH * 0.23, innerH * 0.18]; // 1 > 2 > 3
  const colX = [cx - colW/2, cx - lane - colW/2, cx + lane - colW/2];
  const colY = [podiumY - colHeights[0], podiumY - colHeights[1], podiumY - colHeights[2]];
  const medalColors = ['#ffd530','#9fe7ff','#ff8fda'];
  const edgeColors = ['#fef08a','#9ee7ff','#ff77d6'];

  // podium columns behind cards
  for (let i=0; i<3; i++){
    noStroke();
    fill(8, 14, 36, 125);
    rect(colX[i], colY[i], colW, colHeights[i], 12, 12, 3, 3);
    stroke(edgeColors[i]); strokeWeight(2); noFill();
    rect(colX[i], colY[i], colW, colHeights[i], 12, 12, 3, 3);
    noStroke(); fill('#f6f8ff');
    textAlign(CENTER, CENTER); textStyle(BOLD); textSize(max(14, innerW * 0.038));
    text(String(i+1), colX[i] + colW/2, colY[i] + colHeights[i] - 24);
    if (i === 0){
      fill('#cbd2ff');
      textSize(max(10, innerW * 0.018));
      text(`played ${getDisplayedPlayedCount()}`, colX[i] + colW/2, colY[i] + colHeights[i] - 8);
    }
  }

  const maxCardW = innerW * (isCompactLb ? 0.26 : 0.30);
  const cellSmall = max(3, floor((maxCardW * (isCompactLb ? 0.64 : 0.70)) / cols));
  const cellBig = floor(cellSmall * 1.18);
  const dims = [
    { cell:cellBig, title:max(isCompactLb ? 20 : 26, LB_SIZES[0]*(isCompactLb ? 1.0 : 1.14)), x:cx, y:colY[0] - innerH*(isCompactLb ? 0.17 : 0.20), rank:1 },
    { cell:cellSmall, title:max(isCompactLb ? 13 : 16, LB_SIZES[1]*(isCompactLb ? 0.8 : 0.95)), x:cx - lane, y:colY[1] - innerH*(isCompactLb ? 0.13 : 0.16), rank:2 },
    { cell:cellSmall, title:max(isCompactLb ? 13 : 16, LB_SIZES[2]*(isCompactLb ? 0.8 : 0.95)), x:cx + lane, y:colY[2] - innerH*(isCompactLb ? 0.13 : 0.16), rank:3 }
  ];

  const items=[];
  for (let i=0; i<3; i++){
    if (!source[i]) continue;
    const d = dims[i];
    const thumbW = d.cell * cols;
    const thumbH = d.cell * rows;
    const bw = thumbW + 26;
    const bh = thumbH + d.title * 2.35 + 26;
    const bx = d.x - bw/2;
    const by = d.y;
    const tx = d.x - thumbW/2;
    const ty = by + d.title * 1.9 + 18;
    const lineY = by + 14;
    lbRects.push({x:bx, y:by, w:bw, h:bh});
    items.push({ rec:source[i], i, rank:d.rank, titleSize:d.title, tx, ty, lineY, cell:d.cell, thumbW, thumbH, bx, by, bw, bh });
  }

  updateLbHover(mouseX, mouseY);
  let hi=-1; if (lbActive>=0 && millis()<lbActiveUntil) hi=lbActive; else if (lbHover>=0) hi=lbHover;

  function drawLbItem(it, lifted){
    const {rec,i,rank,titleSize,lineY,cell,thumbW,thumbH,tx,ty,bx,by,bw,bh}=it;
    const rankColor = medalColors[i] || '#d8ddff';
    const edgeColor = edgeColors[i] || '#aab4ff';
    push();
      if (lifted){
        translate(bx+bw/2, by+bh/2-BOUNCE_OFFSET); scale(SCALE_HOVER); translate(-(bx+bw/2), -(by+bh/2));
        noStroke(); fill(0,80); rect(bx-10, by-10, bw+20, bh+20, 12);
      }
      noStroke();
      fill(7, 11, 28, 160);
      rect(bx-8, by-6, bw+16, bh+14, 12);
      noFill();
      stroke(edgeColor);
      strokeWeight(2);
      rect(bx-8, by-6, bw+16, bh+14, 12);

      const nameSize = max(isCompactLb ? 12 : 16, titleSize * 0.78);
      const scoreSize = max(isCompactLb ? 10 : 13, titleSize * 0.48);
      const nameY = lineY + 4;
      const scoreY = nameY + nameSize + 6;
      const displayName = String(rec.name || '');
      noStroke();
      fill(rankColor);
      textSize(nameSize);
      textAlign(CENTER, TOP);
      textStyle(BOLD);
      const maxNamePx = bw - (isCompactLb ? 18 : 56);
      const compactName = fitTextToWidth(displayName, maxNamePx);
      const desktopPrefix = `#${rank}  `;
      const desktopName = desktopPrefix + fitTextToWidth(displayName, Math.max(24, maxNamePx - textWidth(desktopPrefix)));
      text(isCompactLb ? compactName : desktopName, bx + bw/2, nameY);
      fill('#f6f8ff');
      textSize(scoreSize);
      text(`Empty Blocks: ${rec.score}`, bx + bw/2, scoreY);

      // Medal badge
      if (!isCompactLb){
        fill(rankColor);
        circle(bx + bw - 22, by + 18, 24);
        fill('#10131f');
        textAlign(CENTER, CENTER);
        textSize(14);
        text(String(rank), bx + bw - 22, by + 18);
      }

      if (rec.snapshot){
        drawSnapshot(rec.snapshot, tx, ty, cell);
      } else {
        noFill();
        stroke('#FF99B1');
        strokeWeight(2);
        rect(tx, ty, thumbW, thumbH, 6);
      }
    pop();
  }

  for (const it of items){ if (it.i !== hi) drawLbItem(it, false); }
  if (hi >= 0){ const it = items.find(o=>o.i===hi); if (it) drawLbItem(it, true); }
}
function updateLbHover(mx,my){
  lbHover = -1;
  for (let i=0;i<lbRects.length;i++){
    const r = lbRects[i]; if (!r) continue;
    if (mx>=r.x && mx<=r.x+r.w && my>=r.y && my<=r.y+r.h){ lbHover = i; break; }
  }
}

/***** Buttons *****/
function clearButtons(){
  ['#nextPromptBtn','#nextBtn','#savedFlag','#clearBtn','#saveBtn','#makeCharmBtn','#makeCharmBtnLB'].forEach(id=>{ const el=select(id); if(el) el.remove(); });
}
function removeSavedFlag(){ const flag=select('#savedFlag'); if(flag) flag.remove(); }
function createStyledButton(id,label,x,y,onClick){
  const old=select('#'+id); if(old) old.remove();
  const btn=createButton(label); btn.id(id);
  btn.style('position','absolute').style('z-index','9999').style('pointer-events','auto').position(x,y);
  btn.style('background','rgba(0,0,0,.30)').style('color','#ffffff')
     .style('border','1px solid rgba(255,255,255,.14)').style('border-radius','12px').style('cursor','pointer')
     .style('font-weight','800').style('font-family', "Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif")
     .style('backdrop-filter','blur(8px)').style('-webkit-backdrop-filter','blur(8px)');
  const smallIDs=['nextPromptBtn','nextBtn','clearBtn','saveBtn','makeCharmBtn','makeCharmBtnLB'];
  const pad=smallIDs.includes(id)?BTN_PAD_SMALL:BTN_PAD_LARGE; const fz=smallIDs.includes(id)?BTN_FZ_SMALL:BTN_FZ_LARGE;
  btn.style('padding', pad).style('font-size', fz);
  btn.mouseOver(()=>btn.style('border-color','rgba(255,255,255,.28)'));
  btn.mouseOut(()=>btn.style('border-color','rgba(255,255,255,.14)'));
  btn.mousePressed(onClick);
  return btn;
}
function removeIfExists(ref){
  if (ref && typeof ref.remove === 'function') {
    try { ref.remove(); } catch(_) {}
  }
  return null;
}
function openCheckoutConfirmModal(itemName, totalText){
  return new Promise((resolve)=>{
    const mask = createDiv('');
    mask.style('position','fixed').style('inset','0')
      .style('z-index','10080')
      .style('background','rgba(4,8,36,.58)')
      .style('display','flex').style('align-items','center').style('justify-content','center')
      .style('padding','18px');
    const card = createDiv('');
    card.parent(mask);
    card.style('width','min(420px, 94vw)')
      .style('border','1px solid #4a56be')
      .style('border-radius','14px')
      .style('background','#0f1b75')
      .style('color','#f4f6ff')
      .style('padding','14px 14px 12px')
      .style('font-family',"Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif");
    card.elt.innerHTML = `
      <h3 style="margin:0 0 8px;font-size:16px;letter-spacing:.01em;">Confirm Checkout</h3>
      <div style="display:flex;justify-content:space-between;gap:10px;margin:6px 0;font-size:13px;color:#d7ddff;"><span>Item</span><strong>${itemName}</strong></div>
      <div style="display:flex;justify-content:space-between;gap:10px;margin:6px 0;font-size:13px;color:#d7ddff;"><span>Qty</span><strong>1</strong></div>
      <div style="display:flex;justify-content:space-between;gap:10px;margin:6px 0;font-size:13px;color:#d7ddff;"><span>Total</span><strong>${totalText}</strong></div>
      <div style="margin-top:12px;display:flex;justify-content:flex-end;gap:8px;">
        <button type="button" data-cancel style="border:1px solid #4a56be;border-radius:10px;background:#162070;color:#f4f6ff;padding:7px 10px;font-size:11px;font-weight:700;letter-spacing:.04em;cursor:pointer;">Continue</button>
        <button type="button" data-go style="border:1px solid #ee00b8;border-radius:10px;background:#ee00b8;color:#ffffff;padding:7px 10px;font-size:11px;font-weight:700;letter-spacing:.04em;cursor:pointer;">Checkout</button>
      </div>`;
    const cleanup = (ok)=>{ if (mask) mask.remove(); resolve(ok); };
    mask.elt.addEventListener('click', (e)=>{ if (e.target === mask.elt) cleanup(false); });
    const cancelBtn = card.elt.querySelector('[data-cancel]');
    const goBtn = card.elt.querySelector('[data-go]');
    if (cancelBtn) cancelBtn.addEventListener('click', ()=>cleanup(false));
    if (goBtn) goBtn.addEventListener('click', ()=>cleanup(true));
  });
}
function fitTextToWidth(value, maxPx){
  const suffix = '...';
  let s = String(value || '');
  if (maxPx <= 0) return '';
  if (textWidth(s) <= maxPx) return s;
  while (s.length > 1 && textWidth(s + suffix) > maxPx){
    s = s.slice(0, -1);
  }
  return s + suffix;
}

/***** 跑馬燈 *****/
function ensureMarquees(){
  if (IS_MOBILE){
    const phrase = 'INSUFFICIENT SPACE\u00A0';
    const longMsg = phrase.repeat(12);
    const html = `<div class="track"><span class="content">${longMsg}</span><span class="content">${longMsg}</span></div>`;
    if (!mqTop){ mqTop = createDiv(html); mqTop.addClass('mq'); mqTop.addClass('top'); }
    if (!mqBottom){ mqBottom = createDiv(html); mqBottom.addClass('mq'); mqBottom.addClass('bottom'); }
  } else {
    mqTop    = removeIfExists(mqTop);
    mqBottom = removeIfExists(mqBottom);
  }
}
function positionMarquees(){
  if (!IS_MOBILE || !mqTop || !mqBottom) return;
  const barH=max(18, floor(blockSize*0.7)), fontSize=max(12, floor(barH*0.7));
  mqTop.position(0, max(0, canvasY - barH - 6)); mqTop.size(windowWidth, barH); mqTop.style('font-size', fontSize+'px');
  mqBottom.position(0, canvasY + height + 6);     mqBottom.size(windowWidth, barH); mqBottom.style('font-size', fontSize+'px');
}

/***** ========= Fullscreen 3D Preview（Three.js） ========= *****/
let charmFS = { overlay:null, footer:null, closeBtn:null };
let charm3D = { texFront:null };
let threeCtx = null;

function getPartIdFromName(name){
  const n = (name || '').trim();
  if (n === '1' || n === '2' || n === '3') return n;
  const m = n.match(/(?:^|[^0-9])0*([123])(?:[^0-9]|$)/);
  return m ? m[1] : null;
}
function collectPartMeshes(root){
  const parts = { '1':[], '2':[], '3':[], other:[] };
  const allMeshes = [];
  const unnamed = [];
  const meshVolume = (mesh)=>{
    const b = new THREE.Box3().setFromObject(mesh);
    const s = b.getSize(new THREE.Vector3());
    return Math.max(0, s.x * s.y * s.z);
  };
  root.traverse((o)=>{
    if (!o.isMesh) return;
    allMeshes.push(o);
    const id = getPartIdFromName(o.name);
    if (id) parts[id].push(o);
    else {
      parts.other.push(o);
      unnamed.push(o);
    }
  });

  // Fallback / repair: fill missing parts by volume rank.
  const byVolDesc = allMeshes.slice().sort((a,b)=>meshVolume(b)-meshVolume(a));
  const used = new Set(parts['1'].concat(parts['2'], parts['3']));
  const firstUnused = ()=>byVolDesc.find((m)=>!used.has(m)) || null;
  const smallestUnused = ()=>byVolDesc.slice().reverse().find((m)=>!used.has(m)) || null;
  if (!parts['1'].length){
    const m = firstUnused();
    if (m){ parts['1'].push(m); used.add(m); }
  }
  if (!parts['2'].length){
    const m = firstUnused();
    if (m){ parts['2'].push(m); used.add(m); }
  }
  if (!parts['3'].length){
    const m = smallestUnused();
    if (m){ parts['3'].push(m); used.add(m); }
  }
  parts.other = allMeshes.filter((m)=>!used.has(m));
  return parts;
}

// 依當前盤面生成貼圖
function buildCharmTexture(w, h){
  const cell = floor(min(w/cols, h/rows));
  const texW = cell*cols, texH = cell*rows;
  const g = createGraphics(texW, texH);
  g.background(BG_BLUE);
  drawSnapshotToGraphics(g, (lastSnapshot || encodeBoardSnapshot()), 0, 0, cell);
  g.noFill(); g.stroke(PINK); g.strokeWeight(12);
  g.rect(6, 6, texW-8, texH-8);
  return g;
}
function getLocalBounds(root){
  if (!root) return null;
  root.updateWorldMatrix(true, true);
  const invRoot = new THREE.Matrix4().copy(root.matrixWorld).invert();
  const out = new THREE.Box3();
  let has = false;
  root.traverse((o)=>{
    if (!o.isMesh || !o.geometry) return;
    if (!o.geometry.boundingBox) o.geometry.computeBoundingBox();
    const bb = o.geometry.boundingBox && o.geometry.boundingBox.clone();
    if (!bb) return;
    const toRoot = new THREE.Matrix4().multiplyMatrices(invRoot, o.matrixWorld);
    bb.applyMatrix4(toRoot);
    if (!has){ out.copy(bb); has = true; } else out.union(bb);
  });
  return has ? out : null;
}
function makeResultVoxelGroup(snapshot, panelRoot, cubeTemplate){
  if (!snapshot || snapshot.length !== cols * rows || !panelRoot) return null;
  const panelBox = getLocalBounds(panelRoot);
  if (!panelBox || panelBox.isEmpty()) return null;

  const panelSize = panelBox.getSize(new THREE.Vector3());
  const insetRatio = 0.84;
  const availW = panelSize.x * insetRatio;
  const availH = panelSize.y * insetRatio;
  const cell = Math.min(availW / cols, availH / rows);
  if (!isFinite(cell) || cell <= 0) return null;
  const step = cell * 0.88;
  const gridW = (cols - 1) * step + cell;
  const gridH = (rows - 1) * step + cell;

  const startX = panelBox.min.x + (panelSize.x - gridW) / 2 + cell / 2;
  const startY = panelBox.max.y - (panelSize.y - gridH) / 2 - cell / 2;
  const z = panelBox.max.z + Math.max(cell * 0.06, panelSize.z * 0.012);

  let template = cubeTemplate;
  let baseScale = 1;
  if (!template){
    template = new THREE.Mesh(new THREE.BoxGeometry(1,1,1), new THREE.MeshStandardMaterial({ color:'#ffffff' }));
  } else {
    const b = new THREE.Box3().setFromObject(template);
    const s = b.getSize(new THREE.Vector3());
    baseScale = Math.max(s.x, s.y, s.z) || 1;
  }

  const group = new THREE.Group();
  for (let r=0; r<rows; r++){
    for (let c=0; c<cols; c++){
      const idxInSnap = r * cols + c;
      const ch = snapshot[idxInSnap];
      if (ch === '8') continue;
      const colorIdx = ch.charCodeAt(0) - 48;
      if (colorIdx < 0 || colorIdx >= PALETTE.length) continue;

      const voxel = template.clone();
      voxel.position.set(startX + c * step, startY - r * step, z);
      const s = (cell * 0.84) / baseScale;
      voxel.scale.set(s, s, s);
      voxel.rotation.set(Math.PI / 2, 0, 0);
      voxel.traverse((o)=>{
        if (!o.isMesh) return;
        o.material = new THREE.MeshPhysicalMaterial({
          color: PALETTE[colorIdx],
          metalness: 0.14,
          roughness: 0.18,
          clearcoat: 0.78,
          clearcoatRoughness: 0.12
        });
      });
      group.add(voxel);
    }
  }
  group.rotation.set(Math.PI / 2, Math.PI, 0);
  group.position.y += z + panelBox.max.y + cell * 0.12;
  group.position.add(new THREE.Vector3(45, -90, -18));
  return group.children.length ? group : null;
}


function frameObject(object, camera, controls, focusMeshes = null){
  const box = new THREE.Box3();
  if (focusMeshes && focusMeshes.length){
    focusMeshes.forEach((m)=>box.expandByObject(m));
  } else {
    box.setFromObject(object);
  }
  const sphere = box.getBoundingSphere(new THREE.Sphere());
  const center = sphere.center;
  const radius = Math.max(sphere.radius, 1e-6);
  controls.autoRotate = true;
  controls.autoRotateSpeed = 0.38;

  controls.target.copy(center);

  const fov = camera.fov * (Math.PI / 180);
  let dist = radius / Math.sin(fov / 2);
  dist *= 1.62;
  dist = Math.max(dist, 1.5);

  const viewDir = new THREE.Vector3()
    .subVectors(camera.position, controls.target)
    .normalize();
  if (!isFinite(viewDir.lengthSq()) || viewDir.lengthSq() < 1e-6) viewDir.set(0,0,1);
  camera.position.copy(viewDir.multiplyScalar(dist).add(controls.target));

  camera.near = Math.max(dist / 100, 0.001);
  camera.far  = dist * 100;
  camera.updateProjectionMatrix();

  controls.minDistance = dist * 0.25;
  controls.maxDistance = dist * 4.0;
  controls.update();
}

async function initThreeViewer(containerEl, getSnapshotCanvas, modelPath, options = {}){
  if (!window.THREE || !THREE.GLTFLoader || !THREE.OrbitControls) {
    alert('Three.js 尚未載好'); return;
  }

  const w = containerEl.clientWidth, h = containerEl.clientHeight;
  const renderer = new THREE.WebGLRenderer({ antialias:true, alpha:true });
  renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
  renderer.setSize(w, h, false);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 0.98;
  containerEl.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(35, w/h, 0.01, 100);
  camera.position.set(0.72, 0.5, 1.05);

  const controls = new THREE.OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true; controls.enablePan = true; controls.enableRotate = true;
  controls.minDistance = 0.2;    controls.maxDistance = 5;
  const baseAutoRotateSpeed = 0.38;
  let rotateResumeAt = 0;
  controls.addEventListener('start', ()=>{
    controls.autoRotate = false;
  });
  controls.addEventListener('end', ()=>{
    rotateResumeAt = performance.now() + 1000;
  });

  scene.add(new THREE.HemisphereLight(0xffffff, 0x97a3b5, 0.68));
  scene.add(new THREE.AmbientLight(0xffffff, 0.3));
  const keyLight = new THREE.DirectionalLight(0xffffff, 0.78);
  keyLight.position.set(-1.1, 1.4, 1.35);
  scene.add(keyLight);
  const fillLight = new THREE.DirectionalLight(0xecf3ff, 0.62);
  fillLight.position.set(1.4, 0.85, 1.5);
  scene.add(fillLight);
  const rimLight = new THREE.DirectionalLight(0xffffff, 0.3);
  rimLight.position.set(0.0, 1.0, -1.6);
  scene.add(rimLight);
  const leftFill = new THREE.DirectionalLight(0xf6f9ff, 0.24);
  leftFill.position.set(-1.8, 0.35, 0.6);
  scene.add(leftFill);
  const rightFill = new THREE.DirectionalLight(0xf6f9ff, 0.24);
  rightFill.position.set(1.8, 0.35, 0.6);
  scene.add(rightFill);
  const topSoft = new THREE.PointLight(0xffffff, 0.2, 6);
  topSoft.position.set(0, 1.8, 0.9);
  scene.add(topSoft);

  // 材質
  const FRAME_COLOR = '#f1f4fb'; // polished silver tone
  const metalMat = new THREE.MeshPhysicalMaterial({
    color: FRAME_COLOR,
    metalness: 0.92,
    roughness: 0.12,
    clearcoat: 0.9,
    clearcoatRoughness: 0.08,
    envMapIntensity: 1.35
  });

  const USE_SNAPSHOT_PANEL = false; // force all-silver look
  const cvs = getSnapshotCanvas();
  const snapTex = new THREE.CanvasTexture(cvs);
  snapTex.flipY = false; snapTex.colorSpace = THREE.SRGBColorSpace; snapTex.anisotropy = 8;
  const panelMat = new THREE.MeshPhysicalMaterial({
    map: snapTex, metalness: 0.05, roughness: 0.08, clearcoat: 0.08, clearcoatRoughness: 0.25, side: THREE.FrontSide
  });

  // Material presets for named parts 1/2/3
  const blueMat = new THREE.MeshPhysicalMaterial({
    color:'#1F42FF', metalness:0.16, roughness:0.24, clearcoat:0.35, clearcoatRoughness:0.22
  });
  const pinkMat = new THREE.MeshPhysicalMaterial({
    color:'#FF25DA', metalness:0.14, roughness:0.24, clearcoat:0.3, clearcoatRoughness:0.22
  });
  const blackMat = new THREE.MeshPhysicalMaterial({
    color:'#2f3138', metalness:0.08, roughness:0.58, clearcoat:0.12, clearcoatRoughness:0.42
  });
  const cartridgeGrayMat = new THREE.MeshPhysicalMaterial({
    color:'#9aa2af',
    metalness:0.32,
    roughness:0.46,
    clearcoat:0.2,
    clearcoatRoughness:0.26
  });
  const stickerTex = new THREE.TextureLoader().load('./sticker-shop001.png');
  stickerTex.colorSpace = THREE.SRGBColorSpace;
  stickerTex.flipY = false;
  const stickerMat = new THREE.MeshPhysicalMaterial({
    map: stickerTex,
    color: '#ffffff',
    metalness: 0.2,
    roughness: 0.34,
    clearcoat: 0.42,
    clearcoatRoughness: 0.18,
    envMapIntensity: 1.2
  });

  const mode = options.mode || 'charm'; // charm | shop
  const partState = {
    caseVisible: true,   // item 1 (box case)
    showPart3: true,     // item 3 optional (charm mode)
    part3Node: null,     // explicit node visibility control for split file
    panelNode: null,     // panel node for placing result voxels
    stickerMeshes: [],
    cartridgeMeshes: []
  };

  // 載入 GLB（優先三件式；失敗則回退單檔）
  const loader = new THREE.GLTFLoader();
  loader.setCrossOrigin('anonymous');
  const loadGlb = (url)=>new Promise((resolve, reject)=>{
    loader.load(url, resolve, undefined, reject);
  });
  const meshListOf = (obj)=>{
    const arr = [];
    obj.traverse((o)=>{ if (o.isMesh) arr.push(o); });
    return arr;
  };
  let cubeTemplateMesh = null;
  try {
    const cubeGltf = await loadGlb(CUBE_MODEL_URL);
    cubeTemplateMesh = cubeGltf.scene || null;
  } catch (e){
    console.warn('cube.glb not found, using fallback cube geometry');
  }
  const finalizeLoadedRoot = (root, forcedParts = null)=>{
    const preBox = new THREE.Box3().setFromObject(root);
    const preSize = preBox.getSize(new THREE.Vector3());
    const maxDim = Math.max(preSize.x, preSize.y, preSize.z);
    const TARGET_MAX = (mode === 'charm') ? 0.82 : 1.0;
    const scale = maxDim > 0 ? TARGET_MAX / maxDim : 1.0;
    root.scale.setScalar(scale);
    root.updateWorldMatrix(true, true);
    const postBox = new THREE.Box3().setFromObject(root);
    const postCenter = postBox.getCenter(new THREE.Vector3());
    const postSize = postBox.getSize(new THREE.Vector3());
    root.position.sub(postCenter);
    if (mode === 'charm'){
      const isMobileShift = windowWidth <= 700;
      root.position.y += postSize.y * (isMobileShift ? -0.15 : 0.00);
    }

    let parts = forcedParts || { '1':[], '2':[], '3':[], other:[] };
    if (!forcedParts){
      try {
        parts = collectPartMeshes(root);
      } catch (err) {
        console.warn('part parse failed, fallback to single-material model', err);
        root.traverse((o)=>{ if (o.isMesh) parts.other.push(o); });
      }
    }
    const applyParts = ()=>{
      const setList = (arr, mat, visible=true)=>{ arr.forEach((m)=>{ m.material = mat; m.visible = visible; }); };
      if (mode === 'charm'){
        setList(parts['1'], blueMat, partState.caseVisible);
        setList(parts['2'], pinkMat, true);
        setList(parts['3'], blackMat, partState.showPart3);
        setList(parts.other, metalMat, true);
        if (partState.cartridgeMeshes && partState.cartridgeMeshes.length){
          partState.cartridgeMeshes.forEach((m)=>{ m.material = cartridgeGrayMat; m.visible = true; });
        }
        if (partState.stickerMeshes && partState.stickerMeshes.length){
          partState.stickerMeshes.forEach((m)=>{ m.material = cartridgeGrayMat; m.visible = true; });
        }
        if (partState.part3Node){
          partState.part3Node.visible = partState.showPart3;
          partState.part3Node.traverse((o)=>{ if (o.isMesh) o.visible = partState.showPart3; });
        }
      } else {
        setList(parts['1'], metalMat, partState.caseVisible);
        setList(parts['2'], metalMat, true);
        setList(parts['3'], blackMat, true);
        setList(parts.other, metalMat, true);
        if (partState.stickerMeshes && partState.stickerMeshes.length){
          partState.stickerMeshes.forEach((m)=>{ m.material = stickerMat; m.visible = true; });
        }
      }
    };
    applyParts();

    scene.add(root);
    if (mode === 'charm'){
      const snap = lastSnapshot || encodeBoardSnapshot();
      const panelRoot = partState.panelNode || (parts['2'] && parts['2'][0] ? parts['2'][0].parent : null);
      const voxelGroup = makeResultVoxelGroup(snap, panelRoot, cubeTemplateMesh);
      if (voxelGroup && panelRoot) panelRoot.add(voxelGroup);
    }
    // Center orbit controls on the final visible composition.
    frameObject(root, camera, controls, null);

    if (threeCtx){
      threeCtx.partState = partState;
      threeCtx.refreshParts = applyParts;
      threeCtx.hasPart3 = !!partState.part3Node || parts['3'].length > 0;
      threeCtx.hasCase = parts['1'].length > 0;
    }
  };

  (async ()=>{
    try {
      // Primary: show all five models together.
      const [caseGltf, panelGltf, part3Gltf, bodyGltf, stickerGltf] = await Promise.all([
        loadGlb(MODEL_CASE_URL),
        loadGlb(MODEL_PANEL_URL),
        loadGlb(MODEL_PART3_URL),
        loadGlb(modelPath || MODEL_URL),
        loadGlb(MODEL_STICKER_URL),
      ]);
      const root = new THREE.Group();
      root.add(caseGltf.scene);
      root.add(panelGltf.scene);
      root.add(part3Gltf.scene);
      root.add(bodyGltf.scene);
      root.add(stickerGltf.scene);
      partState.part3Node = part3Gltf.scene;
      partState.panelNode = panelGltf.scene;
      partState.cartridgeMeshes = meshListOf(bodyGltf.scene);
      partState.stickerMeshes = meshListOf(stickerGltf.scene);

      const p1 = meshListOf(caseGltf.scene).concat(meshListOf(bodyGltf.scene));
      const p2 = meshListOf(panelGltf.scene);
      const p3 = meshListOf(part3Gltf.scene);
      const used = new Set([].concat(p1, p2, p3));
      const all = meshListOf(root);
      const forcedParts = { '1':p1, '2':p2, '3':p3, other: all.filter((m)=>!used.has(m)) };
      finalizeLoadedRoot(root, forcedParts);
    } catch (allErr){
      console.warn('five-model load failed, fallback to single glb', allErr);
      partState.part3Node = null;
      partState.panelNode = null;
      try {
        const gltf = await loadGlb(modelPath || MODEL_URL);
        finalizeLoadedRoot(gltf.scene, null);
      } catch (err){
        console.error('GLB load failed', err);
        alert('Unable to load all five models (box/cartridge set) or fallback model.');
      }
    }
  })();

  // render loop
  function tick(){
    if (rotateResumeAt && performance.now() >= rotateResumeAt){
      controls.autoRotate = true;
      controls.autoRotateSpeed = baseAutoRotateSpeed;
      rotateResumeAt = 0;
    }
    controls.update();
    renderer.render(scene, camera);
    threeCtx && (threeCtx.rafId = requestAnimationFrame(tick));
  }
  threeCtx = { renderer, scene, camera, controls, containerEl, rafId:null, onResize:null, partState:null, refreshParts:null, hasPart3:false, hasCase:false };
  tick();

  // resize
  function onResize(){
    const ww = containerEl.clientWidth, hh = containerEl.clientHeight;
    renderer.setSize(ww, hh, false);
    camera.aspect = ww / hh;
    camera.updateProjectionMatrix();
  }
  window.addEventListener('resize', onResize);
  threeCtx.onResize = onResize;
}

function disposeThreeViewer(){
  if (!threeCtx) return;
  if (threeCtx.rafId) cancelAnimationFrame(threeCtx.rafId);
  if (threeCtx.onResize) window.removeEventListener('resize', threeCtx.onResize);
  if (threeCtx.renderer) threeCtx.renderer.dispose();
  if (threeCtx.containerEl && threeCtx.containerEl.firstChild){
    threeCtx.containerEl.removeChild(threeCtx.containerEl.firstChild);
  }
  threeCtx = null;
}

async function openCharmPreview3D(options = {}){
  closeCharmPreview3D();
  const fromGameOver = !!options.fromGameOver;
  const isCompactReward = windowWidth <= 700;
  const CHECKOUT_URL = window.CHECKOUT_URL || '';
  charmCloseHook = (typeof options.onClose === 'function') ? options.onClose : null;

  const ov = createDiv('');
  ov.id('charmFSOverlay');
  ov.style('position','fixed').style('inset','0')
    .style('z-index','10050')
    .style('background', fromGameOver ? 'rgba(1,1,1,0.22)' : 'rgba(1,1,1,0.12)')
    .style('backdrop-filter','blur(8px) saturate(1.15)')
    .style('-webkit-backdrop-filter','blur(8px) saturate(1.15)');
  ov.mousePressed((e)=>{ if(e.target===ov.elt) closeCharmPreview3D(); });
  charmFS.overlay = ov;

  if (fromGameOver){
    const fxPulse = createDiv('');
    fxPulse.parent(ov);
    fxPulse.style('position','absolute').style('inset','0')
      .style('z-index','10051')
      .style('pointer-events','none')
      .style('background','radial-gradient(circle at 50% 38%, rgba(255,255,255,0.30) 0%, rgba(245,210,255,0.20) 30%, rgba(27,16,63,0.10) 62%, rgba(0,0,0,0) 100%)')
      .style('mix-blend-mode','screen')
      .style('animation','rewardPulse 1000ms ease-out 2');

    const tetriLayer = createDiv('');
    tetriLayer.parent(ov);
    tetriLayer.style('position','absolute').style('inset','0')
      .style('z-index','10052')
      .style('pointer-events','none')
      .style('overflow','hidden');

    const shapeKeys = Object.keys(SHAPES);
    const tetriCount = isCompactReward ? 22 : 38;
    for (let i=0; i<tetriCount; i++){
      const key = shapeKeys[i % shapeKeys.length];
      const mat = SHAPES[key];
      const unit = 8 + (i % 4) * 2;
      const w = mat[0].length * unit;
      const h = mat.length * unit;
      const piece = createDiv('');
      piece.parent(tetriLayer);
      piece.style('position','absolute')
        .style('left', `${Math.floor(Math.random() * 100)}%`)
        .style('top', '-14%')
        .style('width', `${w}px`)
        .style('height', `${h}px`)
        .style('opacity', '0')
        .style('transform-origin','50% 50%')
        .style('z-index', '10052')
        .style('pointer-events', 'none')
        .style('--drift', `${-120 + Math.random() * 240}px`)
        .style('--rot', `${-220 + Math.random() * 440}deg`)
        .style('--scale', `${0.75 + Math.random() * 0.65}`)
        .style('animation', `rewardTetFall ${1500 + Math.floor(Math.random()*1200)}ms linear infinite`)
        .style('animation-delay', `${-Math.floor(Math.random()*2200)}ms`);
      for (let r=0; r<mat.length; r++){
        for (let c=0; c<mat[r].length; c++){
          if (!mat[r][c]) continue;
          const cell = createDiv('');
          cell.parent(piece);
          const col = PALETTE[(i + r + c) % PALETTE.length];
          cell.style('position','absolute')
            .style('left', `${c * unit}px`)
            .style('top', `${r * unit}px`)
            .style('width', `${unit-1}px`)
            .style('height', `${unit-1}px`)
            .style('border-radius','2px')
            .style('background', col)
            .style('box-shadow', '0 0 8px rgba(255,255,255,0.38)');
        }
      }
    }

    const boom = createDiv('MERCH UNLOCKED');
    boom.parent(ov);
    boom.style('position','absolute')
      .style('left','50%').style('top', isCompactReward ? '8%' : '12%')
      .style('transform','translateX(-50%) rotate(-8deg)')
      .style('z-index','10058')
      .style('pointer-events','none')
      .style('font-family',"Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif")
      .style('font-weight','900')
      .style('font-size', isCompactReward ? 'clamp(20px, 7.2vw, 34px)' : 'clamp(26px, 4.3vw, 58px)')
      .style('letter-spacing', isCompactReward ? '1px' : '2px')
      .style('color','#fff34f')
      .style('text-shadow','-3px 3px 0 #15151c, 0 0 24px rgba(255,244,84,0.95), 0 0 46px rgba(255,255,255,0.65)')
      .style('animation','rewardPopOut 620ms cubic-bezier(0.16, 1, 0.3, 1)');

    const shineFx = createDiv('');
    shineFx.parent(ov);
    shineFx.style('position','absolute')
      .style('left','50%').style('top','40%')
      .style('width', isCompactReward ? '56vmin' : '62vmin')
      .style('height', isCompactReward ? '56vmin' : '62vmin')
      .style('transform','translate(-50%, -50%)')
      .style('z-index','10057')
      .style('pointer-events','none')
      .style('border-radius','999px')
      .style('background','radial-gradient(circle, rgba(255,255,255,0.32) 0%, rgba(255,255,255,0.12) 38%, rgba(255,255,255,0) 72%)')
      .style('mix-blend-mode','screen')
      .style('animation','rewardShine 1800ms ease-in-out infinite');

    const rewardScore = Math.max(0, 1200 - (lastBlocks || 0) * 20);
    const rawDesigner = (lastName || playerName || 'PLAYER').toUpperCase();
    const designerValue = isCompactReward ? rawDesigner.slice(0, 7) : rawDesigner.slice(0, 12);
    const chips = [
      { label:'DESIGNER', value:designerValue, color:'#44f1ff', delay:80 },
      { label:'EMPTY BLOCKS', value:String(lastBlocks || 0), color:'#ff6adf', delay:180 },
      { label:'MERCH SCORE', value:String(rewardScore), color:'#83ff4a', delay:280 }
    ];
    const infoWrap = createDiv('');
    infoWrap.parent(ov);
    infoWrap.style('position','absolute')
      .style('right', isCompactReward ? 'max(8px, 2.2vw)' : 'max(16px, 3.4vw)')
      .style('left', 'auto')
      .style('top', isCompactReward ? 'max(74px, 16vh)' : 'max(72px, 12vh)')
      .style('bottom', 'auto')
      .style('transform', 'none')
      .style('display','flex')
      .style('flex-direction', 'column')
      .style('flex-wrap', 'nowrap')
      .style('justify-content', 'flex-start')
      .style('gap', isCompactReward ? '8px' : '10px')
      .style('width', isCompactReward ? 'min(45vw, 190px)' : 'auto')
      .style('z-index','10061')
      .style('pointer-events','none');
    chips.forEach((chip)=>{
      const card = createDiv(`<span>${chip.label}</span><strong>${chip.value}</strong>`);
      card.parent(infoWrap);
      card.style('display','flex')
        .style('align-items','baseline')
        .style('justify-content','space-between')
        .style('gap', isCompactReward ? '8px' : '12px')
        .style('min-width', isCompactReward ? '120px' : '190px')
        .style('padding', isCompactReward ? '6px 9px' : '8px 12px')
        .style('border-radius','12px')
        .style('background','rgba(7,9,16,0.68)')
        .style('border',`1px solid ${chip.color}`)
        .style('box-shadow',`0 0 0 1px rgba(255,255,255,0.08) inset, 0 0 24px ${chip.color}88`)
        .style('font-family',"Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif")
        .style('opacity','0')
        .style('transform','translateX(34px) scale(0.88)')
        .style('animation','rewardPopCard 700ms cubic-bezier(0.16, 1, 0.3, 1) forwards')
        .style('animation-delay', `${chip.delay}ms`);
      const sp = card.elt.querySelector('span');
      const st = card.elt.querySelector('strong');
      if (sp){
        sp.style.fontWeight = '800';
        sp.style.fontSize = '12px';
        sp.style.letterSpacing = '1.4px';
        sp.style.color = '#f5f7ff';
        sp.style.opacity = '0.92';
      }
      if (st){
        st.style.fontWeight = '900';
        st.style.fontSize = '18px';
        st.style.letterSpacing = '1px';
        st.style.color = chip.color;
      }
    });

    const shardColors = ['#44f1ff','#ffd33d','#ff6adf','#83ff4a','#ffffff','#ff7b29'];
    for (let i=0; i<14; i++){
      const shard = createDiv('');
      const angle = (i / 14) * Math.PI * 2;
      const dist = 140 + (i % 4) * 28;
      shard.parent(ov);
      shard.style('position','absolute')
        .style('left','50%').style('top','38%')
        .style('width', `${12 + (i % 3) * 5}px`)
        .style('height', `${20 + (i % 5) * 6}px`)
        .style('background', shardColors[i % shardColors.length])
        .style('clip-path','polygon(50% 0%, 100% 38%, 78% 100%, 22% 100%, 0% 38%)')
        .style('transform','translate(-50%,-50%)')
        .style('opacity','0')
        .style('mix-blend-mode','screen')
        .style('filter','drop-shadow(0 0 8px rgba(255,255,255,0.35))')
        .style('z-index','10053')
        .style('pointer-events','none')
        .style('--dx', `${Math.cos(angle) * dist}px`)
        .style('--dy', `${Math.sin(angle) * dist}px`)
        .style('--rot', `${-120 + i * 23}deg`)
        .style('animation','rewardShard 880ms ease-out forwards')
        .style('animation-delay', `${70 + i * 18}ms`);
    }
  }

  charm3D.texFront = buildCharmTexture(420*0.82, Math.floor(420*(8/6))*0.82);

  const canvasW = isCompactReward
    ? Math.min(Math.floor(windowWidth * 0.92), Math.floor(windowHeight * 0.48))
    : Math.min(540, Math.floor((windowWidth-50)*0.94));
  const threeWrap = createDiv('');
  threeWrap.parent(ov);
  threeWrap.id('threeWrap');
  threeWrap.style('position','absolute')
    .style('left', isCompactReward ? '32%' : '37%').style('top', '5%')
    .style('transform', fromGameOver
      ? 'translate(-50%, -50%) scale(0.22)'
      : (isCompactReward ? 'translate(-50%, -50%) scale(0.5)' : 'translate(-50%, -50%) scale(0.55)'))
    .style('width', canvasW+'px')
    .style('height', Math.floor(canvasW*(8/6))+'px')
    .style('z-index','10055')
    .style('pointer-events','auto')
    .style('opacity','0')
    .style('will-change','transform, opacity')
    .style('transition', fromGameOver
      ? 'transform 280ms cubic-bezier(0.16, 1, 0.3, 1), opacity 180ms ease-out'
      : 'transform 520ms cubic-bezier(0.2, 0.9, 0.2, 1), opacity 260ms ease-out');

  const footer = createDiv('');
  footer.parent(ov);
  footer.style('position','absolute').style('left','50%')
        .style('bottom', isCompactReward ? '12px' : '20px')
        .style('transform','translateX(-50%)')
        .style('display','flex').style('gap', isCompactReward ? '6px' : '10px').style('flex-wrap','wrap').style('justify-content','center')
        .style('max-width', isCompactReward ? '95vw' : 'none')
        .style('padding', isCompactReward ? '8px 10px' : '10px 12px')
        .style('border','1px solid rgba(255,255,255,0.16)')
        .style('border-radius','12px')
        .style('background','rgba(8,12,46,0.66)')
        .style('z-index','10060').style('pointer-events','auto');
  charmFS.footer = footer;

  const addCart = createButton('Add Cart');
  stylePill(addCart, PALETTE[2], PALETTE[3]);
  addCart.style('background','#ee00b8')
         .style('border-color','#ff61e3')
         .style('color','#ffffff')
         .style('font-weight','800');
  addCart.parent(footer);
  addCart.mousePressed(async ()=> {
    if (!CHECKOUT_URL){
      alert('Checkout URL not set. Please set CHECKOUT_URL.');
      return;
    }
    const ok = await openCheckoutConfirmModal('001 — CCC', '$120');
    if (ok) window.location.href = CHECKOUT_URL;
  });

  const closeBtn = createButton('Close');
  stylePill(closeBtn, '#4C4C4C', '#6A6A6A');
  closeBtn.style('background','rgba(16,23,86,0.92)')
          .style('border-color','rgba(157,171,255,0.55)')
          .style('color','#f3f6ff')
          .style('font-weight','800');
  closeBtn.parent(footer);
  closeBtn.mousePressed(closeCharmPreview3D);
  charmFS.closeBtn = closeBtn;

  const tip = createDiv(fromGameOver ? 'Merch Unlocked' : 'Make your own charm');
  tip.parent(ov);
  tip.style('position','absolute')
     .style('left','50%')
     .style('top', isCompactReward ? '12px' : '18px')
     .style('transform','translateX(-50%)')
     .style('padding', isCompactReward ? '6px 10px' : '7px 12px')
     .style('border','1px solid rgba(255,59,218,0.45)')
     .style('border-radius','999px')
     .style('background','rgba(8,12,46,0.62)')
     .style('font-family',"Montserrat, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans TC', Arial, sans-serif")
     .style('font-size', isCompactReward ? '13px' : '16px').style('font-weight','800')
     .style('letter-spacing', isCompactReward ? '1px' : '2px').style('color', PINK)
     .style('z-index','10060');

  await ensureThreeScripts();
  await initThreeViewer(
    threeWrap.elt,
    () => charm3D.texFront.elt,
    MODEL_URL,
    { mode:'charm' }
  );

  // Slide-in + scale-up intro (small → big) after click
  requestAnimationFrame(()=>{
    requestAnimationFrame(()=>{
      threeWrap.style('opacity','1');
      threeWrap.style('transform', fromGameOver
        ? (isCompactReward ? 'translate(-50%, -50%) scale(0.88)' : 'translate(-50%, -50%) scale(1.14)')
        : 'translate(-50%, -50%) scale(1)');
      if (fromGameOver){
        setTimeout(()=>{
          if (charmFS.overlay && threeWrap){
            threeWrap.style('transition','transform 190ms cubic-bezier(0.2, 0.9, 0.2, 1), opacity 180ms ease-out');
            threeWrap.style('transform','translate(-50%, -50%) scale(1)');
          }
        }, 210);
      }
    });
  });

  window.addEventListener('keydown', escToCloseFS);
}
function escToCloseFS(e){ if(e.key==='Escape') closeCharmPreview3D(); }
function closeCharmPreview3D(){
  window.removeEventListener('keydown', escToCloseFS);
  if (threeCtx) disposeThreeViewer();
  if (charmFS.closeBtn){ charmFS.closeBtn.remove(); charmFS.closeBtn=null; }
  if (charmFS.footer){ charmFS.footer.remove(); charmFS.footer=null; }
  if (charmFS.overlay){ charmFS.overlay.remove(); charmFS.overlay=null; }
  const cb = charmCloseHook;
  charmCloseHook = null;
  if (cb){
    try { cb(); } catch (e){ console.warn('close hook error', e); }
  }
}

// Reward reveal animation keyframes for game-over transition.
if (!document.getElementById('rewardFxStyle')){
  const style = document.createElement('style');
  style.id = 'rewardFxStyle';
  style.textContent = `
    @keyframes rewardPulse {
      0% { opacity: 0; transform: scale(0.96); }
      25% { opacity: 1; transform: scale(1.02); }
      100% { opacity: 0.35; transform: scale(1); }
    }
    @keyframes rewardTetFall {
      0% { opacity: 0; transform: translate3d(0, -14vh, 0) rotate(0deg) scale(var(--scale)); }
      12% { opacity: 0.95; }
      100% { opacity: 0; transform: translate3d(var(--drift), 122vh, 0) rotate(var(--rot)) scale(var(--scale)); }
    }
    @keyframes rewardPopOut {
      0% { opacity: 0; transform: translateX(-50%) rotate(-8deg) scale(0.65); }
      65% { opacity: 1; transform: translateX(-50%) rotate(-8deg) scale(1.12); }
      100% { opacity: 1; transform: translateX(-50%) rotate(-8deg) scale(1); }
    }
    @keyframes rewardPopCard {
      0% { opacity: 0; transform: translateX(34px) scale(0.88); }
      55% { opacity: 1; transform: translateX(-5px) scale(1.05); }
      100% { opacity: 1; transform: translateX(0) scale(1); }
    }
    @keyframes rewardShard {
      0% { opacity: 0; transform: translate(-50%,-50%) scale(0.2) rotate(0deg); }
      20% { opacity: 1; transform: translate(calc(-50% + var(--dx) * 0.38), calc(-50% + var(--dy) * 0.38)) scale(1) rotate(calc(var(--rot) * 0.45)); }
      100% { opacity: 0; transform: translate(calc(-50% + var(--dx)), calc(-50% + var(--dy))) scale(0.9) rotate(var(--rot)); }
    }
    @keyframes rewardShine {
      0% { opacity: 0.42; transform: translate(-50%, -50%) scale(0.92); }
      50% { opacity: 0.88; transform: translate(-50%, -50%) scale(1.06); }
      100% { opacity: 0.42; transform: translate(-50%, -50%) scale(0.92); }
    }
  `;
  document.head.appendChild(style);
}
