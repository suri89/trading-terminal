/**
 * ═══════════════════════════════════════════════════════════════
 * INSTITUTIONAL TRADING TERMINAL — app.js
 * TradingView Lightweight Charts v4 Engine
 * ═══════════════════════════════════════════════════════════════
 *
 * Modules:
 *   ChartManager       — 3-pane synchronized chart instances
 *   WebSocketClient    — Backend relay connection + message dispatch
 *   DrawingToolkit     — H-line, V-line, Ruler drawing tools
 *   HoverInspector     — Crosshair sync + status line updates
 *   ParticipantRibbon  — Canvas-rendered FB/FS/EB/ES ribbon
 *   UIController       — Header, ping badge, timeframe tabs
 */

'use strict';

// ─────────────────────────────────────────────
// CONSTANTS & CONFIG
// ─────────────────────────────────────────────

const WS_URL     = (window.location.protocol === 'http:' || window.location.protocol === 'https:')
  ? `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws`
  : 'ws://localhost:8765';
const SYMBOL     = 'BTC/USDT';
const ASSET_NAME = 'BTC/USDT Binance Perpetual Futures';

const STATE_COLORS = {
  FB: '#00C896',
  FS: '#FF4757',
  EB: '#FFA502',
  ES: '#7C6FFF',
};

const CHART_COLORS = {
  bg:          '#0A0D14',
  border:      'rgba(255,255,255,0.08)',
  crosshair:   'rgba(148,163,184,0.3)',
  text:        '#94A3B8',
  grid:        'rgba(255,255,255,0.04)',
  upWick:      '#00C896',
  downWick:    '#FF4757',
  upBody:      '#00C896',
  downBody:    '#FF4757',
};

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────

let historicalBars  = [];
let latestBar       = null;
let activeTool      = 'cursor';
let drawings        = [];   // { type, price|time, ... }
let rulerState      = null; // { active, x0, y0, bar0 }
let isHovering      = false;
let currentTf       = '1m';
let wsLatency       = 0;
let prevClosePrice  = null;
let openPrice       = null;  // session open (first bar)

// ─────────────────────────────────────────────
// CHART MANAGER
// ─────────────────────────────────────────────

class ChartManager {
  constructor() {
    this.priceChart  = null;
    this.volumeChart = null;
    this.candleSeries   = null;
    this.volumeSeries   = null;
    this.isSyncing   = false;
  }

  /** Common chart options */
  _baseOptions(container) {
    return {
      container,
      layout: {
        background:  { type: 'solid', color: CHART_COLORS.bg },
        textColor:   CHART_COLORS.text,
        fontFamily:  "'JetBrains Mono', monospace",
        fontSize:    11,
      },
      grid: {
        vertLines:   { color: CHART_COLORS.grid, style: 1 },
        horzLines:   { color: CHART_COLORS.grid, style: 1 },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
        vertLine: { color: CHART_COLORS.crosshair, width: 1, style: 3, labelVisible: true },
        horzLine: { color: CHART_COLORS.crosshair, width: 1, style: 3, labelVisible: true },
      },
      rightPriceScale: {
        borderColor: CHART_COLORS.border,
        textColor:   CHART_COLORS.text,
        scaleMargins: { top: 0.05, bottom: 0.05 },
        // Lock width so it doesn't shift the main chart area differently from volume
        minimumWidth: 75,
      },
      timeScale: {
        borderColor:    CHART_COLORS.border,
        timeVisible:    true,
        secondsVisible: false,
        tickMarkFormatter: (time) => {
          const d = new Date(time * 1000);
          return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
        },
      },
      handleScroll:    true,
      handleScale:     true,
    };
  }

  init() {
    const priceContainer  = document.getElementById('price-chart-container');
    const volumeContainer = document.getElementById('volume-chart-container');

    // ── Pane 1: Price Chart ──
    this.priceChart = LightweightCharts.createChart(priceContainer, {
      ...this._baseOptions(priceContainer),
      width:  priceContainer.clientWidth,
      height: priceContainer.clientHeight,
    });

    this.candleSeries = this.priceChart.addCandlestickSeries({
      upColor:          CHART_COLORS.upBody,
      downColor:        CHART_COLORS.downBody,
      borderUpColor:    CHART_COLORS.upWick,
      borderDownColor:  CHART_COLORS.downWick,
      wickUpColor:      CHART_COLORS.upWick,
      wickDownColor:    CHART_COLORS.downWick,
    });

    // ── Pane 2: Volume Chart ──
    this.volumeChart = LightweightCharts.createChart(volumeContainer, {
      ...this._baseOptions(volumeContainer),
      width:  volumeContainer.clientWidth,
      height: volumeContainer.clientHeight,
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
        vertLine: { color: CHART_COLORS.crosshair, width: 1, style: 3, labelVisible: false },
        horzLine: { visible: false, labelVisible: false },
      },
      rightPriceScale: {
        borderColor:  CHART_COLORS.border,
        textColor:    CHART_COLORS.text,
        // autoScale: true makes it re-fit to the visible bars on every pan/zoom,
        // matching TradingView's behavior where the tallest visible bar fills the pane.
        autoScale:    true,
        scaleMargins: { top: 0.08, bottom: 0.0 },
        // Lock width to match price chart exactly, ensuring perfect vertical alignment
        minimumWidth: 75,
      },
      timeScale: {
        borderColor:    CHART_COLORS.border,
        visible:        true,
        timeVisible:    true,
        secondsVisible: false,
        tickMarkFormatter: (time) => {
          const d = new Date(time * 1000);
          return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
        },
      },
    });

    this.volumeSeries = this.volumeChart.addHistogramSeries({
      color:        CHART_COLORS.upBody,
      priceFormat:  { type: 'volume' },
      priceScaleId: 'right',
      // autoscaleInfoProvider makes LWC recalculate the Y-axis from only the
      // currently visible bars, so the tallest bar in the view always fills the
      // pane — exactly matching TradingView's volume panel behavior.
      autoscaleInfoProvider: (original) => {
        const res = original();
        if (res !== null) {
          res.priceRange.minValue = 0;
        }
        return res;
      },
    });

    // Hide volume chart time scale (ribbon takes that role)
    this.volumeChart.timeScale().applyOptions({ visible: false });

    this._syncCharts();
    this._setupResize();
    log('Charts initialized');
  }

  /** Bidirectional sync: pan/zoom any pane → mirrors to the other */
  _syncCharts() {
    const charts = [this.priceChart, this.volumeChart];

    charts.forEach((chart, idx) => {
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (this.isSyncing || !range) return;
        this.isSyncing = true;
        charts.forEach((other, jdx) => {
          if (idx !== jdx) {
            other.timeScale().setVisibleLogicalRange(range);
          }
        });
        // Also sync ribbon canvas
        ribbon.render();
        this.isSyncing = false;
      });
    });
  }

  _setupResize() {
    const ro = new ResizeObserver(() => {
      const pc = document.getElementById('price-chart-container');
      const vc = document.getElementById('volume-chart-container');
      if (pc && this.priceChart)  this.priceChart.resize(pc.clientWidth, pc.clientHeight);
      if (vc && this.volumeChart) this.volumeChart.resize(vc.clientWidth, vc.clientHeight);
      ribbon.render();
    });
    ro.observe(document.getElementById('chart-area'));
  }

  /** Load a full historical dataset */
  loadHistory(bars) {
    if (!bars || bars.length === 0) return;

    const candleData = bars.map(b => ({
      time:  b.time,
      open:  b.open,
      high:  b.high,
      low:   b.low,
      close: b.close,
    }));

    const volumeData = bars.map(b => ({
      time:  b.time,
      value: b.volume,
      color: b.close >= b.open
        ? 'rgba(0,229,153,0.8)'    // #00E599 up
        : 'rgba(255,59,105,0.8)',  // #FF3B69 down
    }));

    this.candleSeries.setData(candleData);
    this.volumeSeries.setData(volumeData);

    // Scroll to the last 120 bars
    this.priceChart.timeScale().scrollToRealTime();
    ribbon.setData(bars);
    ribbon.render();
  }

  /** Update or insert a live candle tick */
  updateLiveBar(bar) {
    const candle = { time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close };
    const vol    = {
      time:  bar.time,
      value: bar.volume,
      color: bar.close >= bar.open
        ? 'rgba(0,229,153,0.8)'    // #00E599 up
        : 'rgba(255,59,105,0.8)',  // #FF3B69 down
    };

    this.candleSeries.update(candle);
    this.volumeSeries.update(vol);

    latestBar = bar;
    if (!isHovering) hoverInspector.updateStatusLine(bar, true);
    ribbon.updateLatest(bar);
    ribbon.render();
  }

  /** Called when a candle definitively closes */
  closeBar(bar) {
    historicalBars.push(bar);
    this.updateLiveBar(bar);
    ribbon.setData(historicalBars);
    ribbon.render();
  }

  getTimeRange() {
    try {
      return this.priceChart.timeScale().getVisibleLogicalRange();
    } catch { return null; }
  }

  getBarCoordinate(time) {
    try {
      return this.priceChart.timeScale().timeToCoordinate(time);
    } catch { return null; }
  }

  getPriceCoordinate(price) {
    try {
      return this.candleSeries.priceToCoordinate(price);
    } catch { return null; }
  }

  coordinateToPrice(y) {
    try {
      return this.candleSeries.coordinateToPrice(y);
    } catch { return null; }
  }
}

// ─────────────────────────────────────────────
// HOVER INSPECTOR
// ─────────────────────────────────────────────

class HoverInspector {
  constructor() {
    this.elCoverage = document.getElementById('sl-coverage');
    this.elOpeners  = document.getElementById('sl-openers');
    this.elClosers  = document.getElementById('sl-closers');
    this.elSignal   = document.getElementById('sl-signal');
    this.elState    = document.getElementById('sl-state');
    this.elOiDelta  = document.getElementById('sl-oi-delta');
    this.elVolume   = document.getElementById('sl-volume');
    this.elUptick   = document.getElementById('sl-uptick');
    this._rafId     = null;
    // Crosshair overlay lines for each pane
    this._volLine    = document.getElementById('volume-crosshair-line');
    this._ribbonLine = null;  // set in attachCrosshairHandlers after DOM ready
  }

  updateStatusLine(bar, isLive = false) {
    if (!bar) return;

    const state    = bar.state || '—';
    const stateKey = state.toLowerCase();   // 'fb', 'fs', 'eb', 'es'

    // Coverage with %
    const coverage = bar.coverage != null
      ? bar.coverage.toFixed(2) + '%'
      : '—';

    // Openers Share with % — color-coded by state
    const openers = bar.openers_share != null
      ? bar.openers_share.toFixed(2) + '%'
      : '—';

    // Closers Share with %
    const closers = bar.closers_share != null
      ? bar.closers_share.toFixed(2) + '%'
      : '—';

    // Net OI Δ with sign and BTC unit
    const oid = bar.oi_delta != null
      ? (bar.oi_delta >= 0 ? '+' : '') + bar.oi_delta.toFixed(2) + ' BTC'
      : '—';

    // Traded Volume in BTC units
    const vol = bar.volume != null
      ? bar.volume.toFixed(2) + ' BTC'
      : '—';

    // Up-Tick % vs Down-Tick % Ratio
    const uptickVal = bar.uptick_pct != null ? bar.uptick_pct : 50.0;
    const uptickTxt = uptickVal.toFixed(1) + '% Up / ' + (100.0 - uptickVal).toFixed(1) + '% Dn';

    // Signal with explicit tag: e.g. "1.00 [FB]"
    const signal = bar.signal != null
      ? bar.signal.toFixed(2) + ' [' + state + ']'
      : '—';

    this.elCoverage.textContent = coverage;
    this.elCoverage.className   = 'status-metric';

    this.elOpeners.textContent  = openers;
    this.elOpeners.className    = 'status-metric metric-' + stateKey;

    if (this.elClosers) {
      this.elClosers.textContent = closers;
      this.elClosers.className   = 'status-metric';
    }

    this.elOiDelta.textContent  = oid;
    this.elOiDelta.className    = 'status-metric';

    if (this.elVolume) {
      this.elVolume.textContent = vol;
      this.elVolume.className   = 'status-metric';
    }

    if (this.elUptick) {
      this.elUptick.textContent = uptickTxt;
      this.elUptick.className   = 'status-metric ' + (uptickVal >= 50.0 ? 'metric-fb' : 'metric-fs');
    }

    this.elSignal.textContent   = signal;
    this.elSignal.className     = 'status-metric metric-' + stateKey;

    // State badge
    this.elState.textContent = state;
    this.elState.className   = 'status-state state-' + state;
  }

  attachCrosshairHandlers(chartManager) {
    // Crosshair overlay line elements in ribbon + volume panes
    this._ribbonLine = document.getElementById('ribbon-crosshair-line');
    // this._volLine already set in constructor

    chartManager.priceChart.subscribeCrosshairMove((param) => {
      if (!param.time || !param.point) {
        isHovering = false;
        if (latestBar) this.updateStatusLine(latestBar, true);
        // Hide both overlay crosshair lines
        if (this._volLine)    this._volLine.style.display    = 'none';
        if (this._ribbonLine) this._ribbonLine.style.display = 'none';
        // Debounced ribbon reset
        if (this._rafId) cancelAnimationFrame(this._rafId);
        this._rafId = requestAnimationFrame(() => { ribbon._activeIdx = -1; ribbon.render(); });
        return;
      }

      isHovering = true;

      // Find the bar in history
      const bar = historicalBars.find(b => b.time === param.time)
                  || (latestBar && latestBar.time === param.time ? latestBar : null)
                  || latestBar;
      if (bar) this.updateStatusLine(bar, false);

      // Compute bar index in historicalBars for ribbon glow
      let barIdx = historicalBars.findIndex(b => b.time === param.time);
      if (barIdx === -1 && latestBar && latestBar.time === param.time) {
        barIdx = historicalBars.length;
      }

      // Get authoritative x pixel from LWC time scale
      let xCoord = null;
      try {
        xCoord = chartManager.priceChart.timeScale().timeToCoordinate(param.time);
      } catch {}

      // Position volume crosshair line
      if (this._volLine && xCoord != null && xCoord >= 0) {
        this._volLine.style.left    = xCoord + 'px';
        this._volLine.style.display = 'block';
      }

      // Position ribbon crosshair line
      if (this._ribbonLine && xCoord != null && xCoord >= 0) {
        this._ribbonLine.style.left    = xCoord + 'px';
        this._ribbonLine.style.display = 'block';
      }

      // Debounced ribbon render with active cell glow
      if (this._rafId) cancelAnimationFrame(this._rafId);
      this._rafId = requestAnimationFrame(() => ribbon.renderWithHover(barIdx));

      // Drawing tool integration
      drawingToolkit.onCrosshairMove(param);
    });
  }
}

// ─────────────────────────────────────────────
// PARTICIPANT RIBBON (Canvas)
// ─────────────────────────────────────────────

class ParticipantRibbon {
  constructor() {
    this.canvas    = document.getElementById('ribbon-canvas');
    this.ctx       = this.canvas.getContext('2d');
    this.bars      = [];
    this.latestBar = null;
    this._activeIdx = -1;   // index into historicalBars being hovered (-1 = none)
  }

  setData(bars) {
    this.bars = [...bars];
  }

  updateLatest(bar) {
    this.latestBar = bar;
  }

  /** Render with a highlighted cell at barIdx. -1 = no highlight. */
  renderWithHover(barIdx) {
    this._activeIdx = barIdx;
    this.render();
  }

  render() {
    const activeIdx = this._activeIdx;
    const canvas = this.canvas;
    const ctx    = this.ctx;
    const pane   = document.getElementById('pane-ribbon');
    const W      = pane.clientWidth;
    const H      = pane.clientHeight;

    // DPR scaling — reset canvas transform each time
    const dpr = window.devicePixelRatio || 1;
    canvas.width  = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width  = W + 'px';
    canvas.style.height = H + 'px';
    ctx.scale(dpr, dpr);

    // Background
    ctx.fillStyle = '#0D1117';
    ctx.fillRect(0, 0, W, H);

    if (!chartManager.priceChart || this.bars.length === 0) return;

    // Get visible logical range to know which bars are on-screen
    const logRange = chartManager.priceChart.timeScale().getVisibleLogicalRange();
    if (!logRange) return;

    const from = Math.max(0, Math.floor(logRange.from));
    const to   = Math.min(this.bars.length - 1, Math.ceil(logRange.to));
    if (to < from) return;

    // Build the render list (swap last entry for live bar if we're at the edge)
    let renderBars = this.bars.slice(from, to + 1);
    if (this.latestBar && to >= this.bars.length - 1) {
      renderBars = [...renderBars.slice(0, -1), this.latestBar];
    }

    // ─── KEY FIX ───────────────────────────────────────────────────────────
    // Use LWC's own timeToCoordinate() for every bar's x-center so that
    // the ribbon cells are pixel-perfect under their corresponding candles,
    // regardless of price-scale width, scroll margins, or bar spacing.
    // ───────────────────────────────────────────────────────────────────────
    const ts = chartManager.priceChart.timeScale();
    const positions = [];
    renderBars.forEach((bar, i) => {
      try {
        const xCenter = ts.timeToCoordinate(bar.time);
        if (xCenter != null) {
          positions.push({ bar, xCenter, absIdx: from + i });
        }
      } catch { /* skip bars that can't be mapped */ }
    });

    if (positions.length === 0) return;

    // Derive cell width from the average spacing between consecutive bar centers
    // (handles variable spacing caused by weekends / missing data gracefully)
    let totalSpacing = 0, spacingCount = 0;
    for (let i = 1; i < positions.length; i++) {
      totalSpacing += positions[i].xCenter - positions[i - 1].xCenter;
      spacingCount++;
    }
    const cellW = spacingCount > 0 ? totalSpacing / spacingCount : W / positions.length;
    const halfCell = cellW / 2;

    const FONT_SIZE = Math.min(10, Math.max(7, cellW * 0.35));
    ctx.font = `600 ${FONT_SIZE}px 'JetBrains Mono', monospace`;
    ctx.textAlign    = 'center';
    ctx.textBaseline = 'middle';

    positions.forEach(({ bar, xCenter, absIdx }) => {
      const state    = bar.state || 'FB';
      const color    = STATE_COLORS[state] || '#666';
      const cellLeft = xCenter - halfCell;
      const isActive = (absIdx === activeIdx);

      if (isActive) {
        // Scale up +15% around the cell center for the glow effect
        const SCALE   = 1.15;
        const scaledW = cellW * SCALE;
        const scaledH = H  * SCALE;
        const ox      = cellLeft - (scaledW - cellW) / 2;
        const oy      = (H - scaledH) / 2;

        // Bright background fill
        ctx.fillStyle = hexToRgba(color, 0.28);
        ctx.fillRect(ox, oy, scaledW, scaledH);

        // 2px glowing white border
        ctx.strokeStyle = 'rgba(255,255,255,0.92)';
        ctx.lineWidth   = 2;
        ctx.shadowColor = color;
        ctx.shadowBlur  = 10;
        ctx.strokeRect(ox + 1, oy + 1, scaledW - 2, scaledH - 2);
        ctx.shadowBlur  = 0;
        ctx.lineWidth   = 1;

        // Bright white label
        if (cellW >= 8) {
          ctx.fillStyle   = '#FFFFFF';
          ctx.shadowColor = color;
          ctx.shadowBlur  = 8;
          ctx.fillText(state, xCenter, H / 2);
          ctx.shadowBlur  = 0;
        }
      } else {
        // Normal cell background
        ctx.fillStyle = hexToRgba(color, 0.12);
        ctx.fillRect(cellLeft, 0, cellW, H);

        // Left border accent
        ctx.fillStyle = hexToRgba(color, 0.3);
        ctx.fillRect(cellLeft, 0, 1, H);

        // Label
        if (cellW >= 12) {
          ctx.fillStyle = color;
          ctx.fillText(state, xCenter, H / 2);
        }
      }
    });

    // Top separator line
    ctx.fillStyle = 'rgba(255,255,255,0.12)';
    ctx.fillRect(0, 0, W, 1);
  }
}

// ─────────────────────────────────────────────
// DRAWING TOOLKIT
// ─────────────────────────────────────────────

class DrawingToolkit {
  constructor() {
    this.drawings   = [];  // { id, type, price, time, ... }
    this.svg        = null;
    this.rulerActive = false;
    this.rulerStart = null;
    this.nextId     = 1;
    this._init();
  }

  _init() {
    // Create SVG overlay on price pane
    const pane = document.getElementById('pane-price');
    this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    this.svg.setAttribute('class', 'drawing-svg');
    this.svg.style.position = 'absolute';
    this.svg.style.inset    = '0';
    this.svg.style.pointerEvents = 'none';
    this.svg.style.zIndex   = '15';
    pane.appendChild(this.svg);

    // Ruler rect
    this.rulerRect = document.getElementById('ruler-rect');
    this.measureBox = null;

    // Click handler on price pane
    document.getElementById('price-chart-container').addEventListener('click', (e) => {
      if (activeTool === 'hline') this._placeHLine(e);
      if (activeTool === 'vline') this._placeVLine(e);
    });

    // Ruler mouse events
    const container = document.getElementById('pane-price');
    container.addEventListener('mousedown', (e) => {
      if (activeTool === 'ruler') this._rulerStart(e);
    });
    container.addEventListener('mousemove', (e) => {
      if (activeTool === 'ruler' && this.rulerActive) this._rulerMove(e);
    });
    container.addEventListener('mouseup',  (e) => {
      if (activeTool === 'ruler') this._rulerEnd(e);
    });
  }

  _placeHLine(e) {
    const rect  = e.currentTarget.getBoundingClientRect();
    const y     = e.clientY - rect.top;
    const price = chartManager.coordinateToPrice(y);
    if (price == null) return;

    const id = this.nextId++;
    this.drawings.push({ id, type: 'hline', price });
    this._render();
  }

  _placeVLine(e) {
    // For VLines we store the x coordinate (time-based) 
    // We'll use the logical coordinate approach
    const rect = e.currentTarget.getBoundingClientRect();
    const x    = e.clientX - rect.left;
    // Convert x to time via timeScale
    try {
      const time = chartManager.priceChart.timeScale().coordinateToTime(x);
      if (!time) return;
      const id = this.nextId++;
      this.drawings.push({ id, type: 'vline', time });
      this._render();
    } catch {}
  }

  onCrosshairMove(param) {
    if (activeTool !== 'ruler' || !this.rulerActive) return;
  }

  _rulerStart(e) {
    const rect  = document.getElementById('price-chart-container').getBoundingClientRect();
    const x     = e.clientX - rect.left;
    const y     = e.clientY - rect.top;
    const price = chartManager.coordinateToPrice(y);
    const time  = (() => { try { return chartManager.priceChart.timeScale().coordinateToTime(x); } catch { return null; } })();
    if (!price || !time) return;

    this.rulerActive = true;
    this.rulerStart  = { x, y, price, time, barIdx: this._timeToBarIdx(time) };

    this.rulerRect.style.display = 'block';
    this.rulerRect.style.left    = x + 'px';
    this.rulerRect.style.top     = y + 'px';
    this.rulerRect.style.width   = '0';
    this.rulerRect.style.height  = '0';
  }

  _rulerMove(e) {
    if (!this.rulerStart) return;
    const rect  = document.getElementById('price-chart-container').getBoundingClientRect();
    const x     = e.clientX - rect.left;
    const y     = e.clientY - rect.top;

    const x0 = this.rulerStart.x, y0 = this.rulerStart.y;
    this.rulerRect.style.left   = Math.min(x, x0) + 'px';
    this.rulerRect.style.top    = Math.min(y, y0) + 'px';
    this.rulerRect.style.width  = Math.abs(x - x0) + 'px';
    this.rulerRect.style.height = Math.abs(y - y0) + 'px';

    // Live measure tooltip
    const price2 = chartManager.coordinateToPrice(y);
    const time2  = (() => { try { return chartManager.priceChart.timeScale().coordinateToTime(x); } catch { return null; } })();
    if (!price2 || !time2) return;

    const priceDiff = price2 - this.rulerStart.price;
    const pctDiff   = (priceDiff / this.rulerStart.price) * 100;
    const barIdx2   = this._timeToBarIdx(time2);
    const barCount  = Math.abs(barIdx2 - this.rulerStart.barIdx);

    this._showMeasureBox(
      x + rect.left + 12,
      y + rect.top + 8,
      priceDiff,
      pctDiff,
      barCount
    );
  }

  _rulerEnd(e) {
    this.rulerActive = false;
    this.rulerRect.style.display = 'none';
    this._hideMeasureBox();
  }

  _timeToBarIdx(time) {
    if (!time || historicalBars.length === 0) return 0;
    const idx = historicalBars.findIndex(b => b.time >= time);
    return idx === -1 ? historicalBars.length : idx;
  }

  _showMeasureBox(screenX, screenY, priceDiff, pctDiff, barCount) {
    let box = document.getElementById('measure-box-el');
    if (!box) {
      box = document.createElement('div');
      box.id = 'measure-box-el';
      box.className = 'measure-box';
      document.getElementById('pane-price').appendChild(box);
    }

    const priceSign = priceDiff >= 0 ? '+' : '';
    const pctSign   = pctDiff   >= 0 ? '+' : '';
    const cls       = priceDiff >= 0 ? 'measure-positive' : 'measure-negative';

    box.innerHTML = `
      <div class="measure-row">
        <span class="measure-key">Δ Price</span>
        <span class="measure-val ${cls}">${priceSign}$${Math.abs(priceDiff).toFixed(2)}</span>
      </div>
      <div class="measure-row">
        <span class="measure-key">Return</span>
        <span class="measure-val ${cls}">${pctSign}${pctDiff.toFixed(3)}%</span>
      </div>
      <div class="measure-row">
        <span class="measure-key">Bars</span>
        <span class="measure-val">${barCount}</span>
      </div>
    `;

    const paneRect = document.getElementById('pane-price').getBoundingClientRect();
    box.style.left    = (screenX - paneRect.left) + 'px';
    box.style.top     = (screenY - paneRect.top) + 'px';
    box.style.display = 'block';
  }

  _hideMeasureBox() {
    const box = document.getElementById('measure-box-el');
    if (box) box.style.display = 'none';
  }

  _render() {
    // Clear SVG
    while (this.svg.firstChild) this.svg.removeChild(this.svg.firstChild);

    const pane  = document.getElementById('pane-price');
    const W     = pane.clientWidth;
    const H     = pane.clientHeight;
    this.svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    this.svg.setAttribute('width', W);
    this.svg.setAttribute('height', H);

    this.drawings.forEach(d => {
      if (d.type === 'hline') this._renderHLine(d, W, H);
      if (d.type === 'vline') this._renderVLine(d, W, H);
    });
  }

  _renderHLine(d, W, H) {
    const y = chartManager.getPriceCoordinate(d.price);
    if (y == null || y < 0 || y > H) return;

    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('class',   'h-line');
    line.setAttribute('x1',      0);
    line.setAttribute('x2',      W);
    line.setAttribute('y1',      y);
    line.setAttribute('y2',      y);
    this.svg.appendChild(line);

    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    text.setAttribute('class', 'h-line-label');
    text.setAttribute('x',     W - 6);
    text.setAttribute('y',     y - 4);
    text.setAttribute('text-anchor', 'end');
    text.setAttribute('font-family', 'JetBrains Mono, monospace');
    text.setAttribute('font-size',   '10');
    text.setAttribute('fill',        'rgba(251,191,36,0.9)');
    text.textContent = '$' + d.price.toFixed(2);
    this.svg.appendChild(text);
  }

  _renderVLine(d, W, H) {
    const x = (() => { try { return chartManager.priceChart.timeScale().timeToCoordinate(d.time); } catch { return null; } })();
    if (x == null || x < 0 || x > W) return;

    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('class', 'v-line');
    line.setAttribute('x1',    x);
    line.setAttribute('x2',    x);
    line.setAttribute('y1',    0);
    line.setAttribute('y2',    H);
    this.svg.appendChild(line);
  }

  clearAll() {
    this.drawings = [];
    this._render();
    this._hideMeasureBox();
    if (this.rulerRect) this.rulerRect.style.display = 'none';
  }

  rerender() {
    this._render();
  }
}

// ─────────────────────────────────────────────
// UI CONTROLLER
// ─────────────────────────────────────────────

class UIController {
  constructor() {
    this.priceEl    = document.getElementById('live-price');
    this.changeEl   = document.getElementById('price-change');
    this.pingEl     = document.getElementById('ping-value');
    this.connDotEl  = document.getElementById('conn-dot');
    this.connTextEl = document.getElementById('conn-text');
    this.loaderEl   = document.getElementById('loading-overlay');
    this.loaderMsg  = document.getElementById('loader-status');
    this._lastPrice = null;
  }

  updatePrice(price) {
    if (price == null) return;
    const formatted = '$' + price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    this.priceEl.textContent = formatted;

    if (openPrice == null) openPrice = price;
    const diff    = price - openPrice;
    const pct     = (diff / openPrice) * 100;
    const sign    = diff >= 0 ? '+' : '';
    const cls     = diff >= 0 ? 'price-up' : 'price-down';
    this.changeEl.textContent = `${sign}${pct.toFixed(2)}%`;
    this.changeEl.className   = cls;

    this._lastPrice = price;
  }

  updatePing(ms) {
    this.pingEl.textContent = `Direct WebSocket: ${ms}ms (Zero-Lag)`;
  }

  setPingConnected() {
    if (this.pingEl.textContent === 'Establishing connection...') {
      this.pingEl.textContent = 'Connected · Awaiting live feed';
    }
  }

  setConnState(state) {
    // state: 'connecting' | 'connected' | 'loading' | 'error'
    const dot  = this.connDotEl;
    const text = this.connTextEl;
    dot.className = 'conn-dot';

    const states = {
      connecting: { cls: 'loading', label: 'Connecting...' },
      connected:  { cls: 'connected', label: 'Live' },
      loading:    { cls: 'loading', label: 'Loading history...' },
      error:      { cls: 'error',   label: 'Disconnected' },
    };

    const s = states[state] || states.error;
    dot.classList.add(s.cls);
    text.textContent = s.label;
  }

  setLoaderMessage(msg) {
    if (this.loaderMsg) this.loaderMsg.textContent = msg;
  }

  hideLoader() {
    if (this.loaderEl) {
      this.loaderEl.classList.add('hidden');
      setTimeout(() => { this.loaderEl.style.display = 'none'; }, 600);
    }
  }

  setActiveTab(tf) {
    document.querySelectorAll('.tf-tab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tf === tf);
    });
  }

  setActiveTool(tool) {
    document.querySelectorAll('.tool-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tool === tool);
    });
    activeTool = tool;

    // Change cursor
    const cc = document.getElementById('price-chart-container');
    const cursors = {
      cursor: 'default',
      hline:  'crosshair',
      vline:  'crosshair',
      ruler:  'crosshair',
    };
    cc.style.cursor = cursors[tool] || 'default';
  }
}

// ─────────────────────────────────────────────
// COUNTDOWN TIMER
// ─────────────────────────────────────────────

const TIMEFRAME_SECONDS = {
  '1m':  60,
  '3m':  180,
  '5m':  300,
  '15m': 900,
  '1H':  3600,
  '1D':  86400,
};

class CountdownTimer {
  constructor() {
    this.badge      = document.getElementById('countdown-badge');
    this.valueEl    = document.getElementById('countdown-value');
    this._interval  = null;
    this._tf        = '1m';
  }

  /** Start (or restart) the timer for a given timeframe. */
  start(tf) {
    this._tf = tf;
    if (this._interval) clearInterval(this._interval);
    this._tick();
    this._interval = setInterval(() => this._tick(), 1000);
  }

  /** Call whenever a new bar closes so the UI resets cleanly. */
  onBarClose() {
    // The tick math already handles this automatically via modulo,
    // but we force an immediate update for responsiveness.
    this._tick();
  }

  _tick() {
    const tf      = this._tf;
    const total   = TIMEFRAME_SECONDS[tf] || 60;
    const nowMs   = Date.now();
    const elapsed = Math.floor(nowMs / 1000) % total;
    const remaining = total - elapsed;

    const mins = Math.floor(remaining / 60);
    const secs = remaining % 60;
    const label = String(mins).padStart(2, '0') + ':' + String(secs).padStart(2, '0');

    if (this.valueEl) this.valueEl.textContent = label;

    // Urgent styling when < 10 seconds
    if (this.badge) {
      this.badge.classList.toggle('urgent', remaining <= 10);
    }
  }
}

// ─────────────────────────────────────────────
// WEBSOCKET CLIENT
// ─────────────────────────────────────────────

class WebSocketClient {
  constructor() {
    this.ws         = null;
    this.reconnectMs = 3000;
    this.reconnecting = false;
  }

  connect() {
    ui.setConnState('connecting');
    ui.setLoaderMessage('Connecting to local server...');

    try {
      this.ws = new WebSocket(WS_URL);
    } catch (e) {
      this._scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      log('WebSocket connected to backend');
      ui.setConnState('connected');
      this.reconnecting = false;
    };

    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        this._dispatch(msg);
      } catch (e) {
        console.error('WS parse error:', e);
      }
    };

    this.ws.onclose = () => {
      log('WebSocket closed');
      ui.setConnState('error');
      this._scheduleReconnect();
    };

    this.ws.onerror = (e) => {
      console.error('WebSocket error:', e);
    };
  }

  _dispatch(msg) {
    switch (msg.type) {

      case 'loading':
        ui.setConnState('loading');
        ui.setLoaderMessage(`Fetching ${msg.tf} history from Binance...`);
        break;

      case 'history': {
        historicalBars = msg.data || [];
        chartManager.loadHistory(historicalBars);
        currentTf = msg.tf;
        ui.setActiveTab(currentTf);
        ui.setConnState('connected');
        ui.setPingConnected();
        ui.hideLoader();
        countdown.start(currentTf);

        if (historicalBars.length > 0) {
          const last = historicalBars[historicalBars.length - 1];
          latestBar  = last;
          openPrice  = historicalBars[0]?.open || last.open;
          ui.updatePrice(last.close);
          hoverInspector.updateStatusLine(last, false);
        }
        log(`History loaded: ${historicalBars.length} bars`);
        break;
      }

      case 'bar_update': {
        const bar = msg.data;
        chartManager.updateLiveBar(bar);
        ui.updatePrice(bar.close);
        if (bar.latency_ms) wsLatency = bar.latency_ms;
        // Reset stale detector — we received live data
        this._resetStaleTimer();
        break;
      }

      case 'bar_close': {
        const bar = msg.data;
        chartManager.closeBar(bar);
        prevClosePrice = bar.close;
        countdown.onBarClose();
        break;
      }

      case 'ping':
        wsLatency = msg.latency_ms;
        ui.updatePing(msg.latency_ms);
        break;

      case 'error':
        console.error('Server error:', msg.msg);
        ui.setLoaderMessage('Error: ' + msg.msg);
        break;
    }
  }

  sendTimeframe(tf) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ action: 'set_timeframe', tf }));
      currentTf = tf;
      historicalBars = [];
      latestBar = null;
      document.getElementById('loading-overlay').style.display = 'flex';
      document.getElementById('loading-overlay').classList.remove('hidden');
      ui.setLoaderMessage(`Switching to ${tf}...`);
    }
  }

  /** Start/reset the stale-stream detector.
   *  If no bar_update arrives within STALE_MS, we assume the Binance feed
   *  has silently frozen and force a reconnect to get fresh history.
   */
  _resetStaleTimer() {
    if (this._staleTimer) clearTimeout(this._staleTimer);
    // 90 s is safe for all timeframes (1m-1D) — BTCUSDT perp trades every ~100ms
    this._staleTimer = setTimeout(() => {
      log('Stale stream detected — reconnecting for fresh data...');
      if (this.ws) {
        try { this.ws.close(); } catch {}
      }
      // _scheduleReconnect will fire via ws.onclose
    }, 90_000);
  }

  _scheduleReconnect() {
    if (this.reconnecting) return;
    this.reconnecting = true;
    setTimeout(() => {
      log('Reconnecting...');
      this.connect();
    }, this.reconnectMs);
  }
}

// ─────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────

function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function log(msg) {
  console.log(`[Terminal] ${msg}`);
}

// ─────────────────────────────────────────────
// MODULE INSTANCES
// ─────────────────────────────────────────────

const chartManager    = new ChartManager();
const ribbon          = new ParticipantRibbon();
const hoverInspector  = new HoverInspector();
const drawingToolkit  = new DrawingToolkit();
const ui              = new UIController();
const wsClient        = new WebSocketClient();
const countdown       = new CountdownTimer();

// ─────────────────────────────────────────────
// TOOLBAR WIRING
// ─────────────────────────────────────────────

function initToolbar() {
  document.querySelectorAll('.tool-btn[data-tool]').forEach(btn => {
    btn.addEventListener('click', () => {
      const tool = btn.dataset.tool;
      if (tool === 'clear') {
        drawingToolkit.clearAll();
        return;
      }
      ui.setActiveTool(tool);
    });
  });
}

// ─────────────────────────────────────────────
// TIMEFRAME TABS
// ─────────────────────────────────────────────

function initTimeframeTabs() {
  document.querySelectorAll('.tf-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const tf = btn.dataset.tf;
      if (tf === currentTf) return;
      wsClient.sendTimeframe(tf);
    });
  });
}

// Redraw SVG drawings on chart pan/zoom
function attachDrawingRerender() {
  chartManager.priceChart.timeScale().subscribeVisibleLogicalRangeChange(() => {
    drawingToolkit.rerender();
  });
  chartManager.priceChart.subscribeCrosshairMove(() => {
    drawingToolkit.rerender();
  });
}

// ─────────────────────────────────────────────
// BOOTSTRAP
// ─────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  log('Initializing terminal...');

  // Init chart manager (creates LWC instances)
  chartManager.init();

  // Attach crosshair → status line
  hoverInspector.attachCrosshairHandlers(chartManager);

  // Wire toolbar + tabs
  initToolbar();
  initTimeframeTabs();
  attachDrawingRerender();

  // Connect to Python backend
  wsClient.connect();

  log('Terminal bootstrap complete');
});
