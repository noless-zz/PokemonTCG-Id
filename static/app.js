/**
 * app.js – PokemonTCG webcam scanner frontend.
 *
 * Features
 * --------
 * - Live camera preview (webcam or phone camera).
 * - Manual capture button + auto-scan toggle (debounced, every 2 s).
 * - Perspective-crop hint overlay.
 * - Quality feedback (blur / no-card warnings).
 * - Top match results with card art, confidence badge, and price.
 */

'use strict';
window.__PTCG_APP_BOOTED__ = true;

/* ── Constants ──────────────────────────────────────────────────────────── */
const AUTO_SCAN_INTERVAL_MS = 2000;
const CAPTURE_JPEG_QUALITY  = 0.85;
const MAX_CAPTURE_WIDTH     = 640;   // down-sample before sending to server
const runtimeConfig = window.POKEMON_TCG_CONFIG || {};
const API_BASE_URL = String(runtimeConfig.apiBaseUrl || '').replace(/\/+$/, '');

/* ── DOM refs ────────────────────────────────────────────────────────────── */
const video       = document.getElementById('video');
const canvas      = document.getElementById('canvas');
const btnCapture  = document.getElementById('btn-capture');
const btnFlip     = document.getElementById('btn-cam-flip');
const btnAuto     = document.getElementById('btn-auto');
const spinner     = document.getElementById('spinner');
const statusBadge = document.getElementById('status-badge');
const qualityBar  = document.getElementById('quality-bar');
const qualityIcon = document.getElementById('quality-icon');
const qualityText = document.getElementById('quality-text');
const resultsEl   = document.getElementById('results');
const noMatchEl   = document.getElementById('no-match');
const ctx         = canvas.getContext('2d');

/* ── State ───────────────────────────────────────────────────────────────── */
let currentStream   = null;
let facingMode      = 'environment';   // 'environment'=back, 'user'=front
let autoScanTimer   = null;
let scanning        = false;

function isGitHubPagesHost() {
  return /(^|\.)github\.io$/i.test(window.location.hostname);
}

function requiresExplicitApiBaseUrl() {
  if (typeof runtimeConfig.requireExplicitApiBaseUrl === 'boolean') {
    return runtimeConfig.requireExplicitApiBaseUrl;
  }
  return isGitHubPagesHost();
}

function hasApiConfigured() {
  return Boolean(API_BASE_URL) || !requiresExplicitApiBaseUrl();
}

function apiUrl(path) {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

function cameraErrorMessage(err) {
  if (err?.name === 'NotAllowedError' || err?.name === 'PermissionDeniedError') {
    return 'Camera permission denied. Allow camera access and reload.';
  }
  if (err?.name === 'NotFoundError' || err?.name === 'DevicesNotFoundError') {
    return 'No camera detected on this device.';
  }
  if (err?.name === 'NotReadableError' || err?.name === 'TrackStartError') {
    return 'Camera is in use by another app. Close it and retry.';
  }
  return `Camera error: ${err?.message ?? 'Unknown error'}`;
}

/* ── Camera helpers ──────────────────────────────────────────────────────── */

async function startCamera(facing = 'environment') {
  if (!navigator.mediaDevices || typeof navigator.mediaDevices.getUserMedia !== 'function') {
    setStatus('Camera not supported in this browser. Use a recent Chrome/Firefox/Safari.', '#e94560');
    btnCapture.disabled = true;
    return;
  }
  if (!window.isSecureContext) {
    setStatus('Camera requires HTTPS (or localhost).', '#e94560');
    btnCapture.disabled = true;
    return;
  }
  if (currentStream) {
    currentStream.getTracks().forEach(t => t.stop());
  }
  try {
    const constraints = {
      video: {
        facingMode: { ideal: facing },
        width:  { ideal: 1280 },
        height: { ideal: 720 },
      },
    };
    currentStream = await navigator.mediaDevices.getUserMedia(constraints);
    video.srcObject = currentStream;
    await video.play();
    if (hasApiConfigured()) {
      setStatus('Ready – point camera at a card', '#8ec6ff');
    } else {
      setStatus('Camera ready. Configure static/config.js apiBaseUrl to enable scanning.', '#ff9800');
    }
    btnCapture.disabled = false;
  } catch (err) {
    setStatus(cameraErrorMessage(err), '#e94560');
    console.error('Camera error:', err);
    btnCapture.disabled = true;
  }
}

btnFlip.addEventListener('click', () => {
  facingMode = facingMode === 'environment' ? 'user' : 'environment';
  startCamera(facingMode);
});

/* ── Capture helpers ─────────────────────────────────────────────────────── */

function captureFrame() {
  const vw = video.videoWidth  || 640;
  const vh = video.videoHeight || 480;
  const scale = Math.min(1, MAX_CAPTURE_WIDTH / vw);
  canvas.width  = Math.round(vw * scale);
  canvas.height = Math.round(vh * scale);
  ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
  return new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', CAPTURE_JPEG_QUALITY));
}

/* ── API call ────────────────────────────────────────────────────────────── */

async function scanCard() {
  if (scanning) return;
  if (!hasApiConfigured()) {
    setStatus(
      'API not configured for GitHub Pages. Define window.POKEMON_TCG_CONFIG.apiBaseUrl (or edit static/config.js).',
      '#ff9800',
    );
    return;
  }
  scanning = true;
  spinner.classList.add('active');
  btnCapture.disabled = true;

  try {
    const blob = await captureFrame();
    const formData = new FormData();
    formData.append('file', blob, 'frame.jpg');

    const res = await fetch(apiUrl('/api/identify'), { method: 'POST', body: formData });
    if (!res.ok) {
      throw new Error(`Server error ${res.status}`);
    }
    const data = await res.json();
    renderResults(data);
  } catch (err) {
    setStatus(`Error: ${err.message}`, '#e94560');
    console.error(err);
  } finally {
    scanning = false;
    spinner.classList.remove('active');
    btnCapture.disabled = false;
  }
}

/* ── Results rendering ───────────────────────────────────────────────────── */

function renderResults(data) {
  // Quality bar.
  if (data.quality_ok) {
    qualityBar.className = 'ok visible';
    qualityIcon.textContent = '✅';
    qualityText.textContent = data.card_detected
      ? 'Card detected and sharp.'
      : 'Image sharp (no card outline found – using full frame).';
  } else {
    qualityBar.className = 'warn visible';
    qualityIcon.textContent = '⚠️';
    qualityText.textContent = `Image quality issue: ${data.quality_reason}. Hold steady.`;
  }

  const matches = data.matches || [];
  resultsEl.innerHTML = '';
  noMatchEl.style.display = 'none';

  if (matches.length === 0) {
    noMatchEl.style.display = 'block';
    setStatus('No match found', '#ff9800');
    return;
  }

  const best = matches[0];
  setStatus(
    `Best match: ${best.card?.name ?? best.card_id} (${(best.confidence * 100).toFixed(0)}%)`,
    best.confidence >= 0.7 ? '#8fffa0' : '#ffd770',
  );

  matches.forEach((m, idx) => {
    const card     = m.card    || {};
    const price    = m.best_price;
    const confPct  = (m.confidence * 100).toFixed(0);
    const imgSrc   = card.small_image || '';
    const priceStr = price
      ? `${price.currency ?? 'USD'} $${Number(price.price).toFixed(2)} (${price.source})`
      : 'No price data';

    const el = document.createElement('div');
    el.className = 'result-card' + (idx === 0 ? ' best' : '');
    el.innerHTML = `
      <img src="${escHtml(imgSrc)}" alt="${escHtml(card.name ?? '')}" loading="lazy"
           onerror="this.style.display='none'" />
      <div class="result-info">
        <div class="result-name">${escHtml(card.name ?? m.card_id)}</div>
        <div class="result-set">${escHtml(card.set_name ?? '')} · ${escHtml(card.number ?? '')}</div>
        <div class="badge-row">
          <span class="badge badge-conf">🎯 ${confPct}% match</span>
          <span class="badge ${price ? 'badge-price' : 'badge-warn'}">
            💰 ${escHtml(priceStr)}
          </span>
        </div>
      </div>
    `;
    // Click card row → open detail page.
    el.style.cursor = 'pointer';
    el.addEventListener('click', () => {
      if (m.card_id) {
        window.open(apiUrl(`/api/cards/${encodeURIComponent(m.card_id)}`), '_blank');
      }
    });
    resultsEl.appendChild(el);
  });
}

/* ── Auto-scan toggle ────────────────────────────────────────────────────── */

btnAuto.addEventListener('click', () => {
  if (autoScanTimer) {
    clearInterval(autoScanTimer);
    autoScanTimer = null;
    btnAuto.textContent = '⚡ Auto-Scan';
    btnAuto.classList.remove('active');
  } else {
    autoScanTimer = setInterval(scanCard, AUTO_SCAN_INTERVAL_MS);
    btnAuto.textContent = '⏹ Stop Auto';
    btnAuto.classList.add('active');
    scanCard();   // scan immediately on enable
  }
});

btnCapture.addEventListener('click', scanCard);

/* ── Utilities ───────────────────────────────────────────────────────────── */

function setStatus(msg, color = '#eaeaea') {
  statusBadge.textContent = msg;
  statusBadge.style.color = color;
}

function escHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ── Initialise ──────────────────────────────────────────────────────────── */
if (isGitHubPagesHost() && !hasApiConfigured()) {
  setStatus('GitHub Pages mode: configure static/config.js apiBaseUrl to enable scanning.', '#ff9800');
}
if (isGitHubPagesHost() && !window.__POKEMON_TCG_CONFIG_LOADED__) {
  setStatus('Config failed to load (static/config.js). Verify GitHub Pages static paths.', '#e94560');
} else {
  startCamera(facingMode);
}
