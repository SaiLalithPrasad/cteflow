"""
renderer.py — Take a graph JSON and produce a self-contained interactive HTML.

Usage (standalone):
    python -m cteflow.renderer <graph.json> [-o output.html]
"""

import html as html_mod
import json
import sys
from pathlib import Path

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SQL Flow &mdash; %%FILENAME%%</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { height: 100%; }
  body {
    height: 100%;
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0b0b14;
    color: #e0e0e0;
    overflow: hidden;
  }

  /* ---- header ---- */
  .header {
    position: fixed; top: 0; left: 0; right: 0; z-index: 100;
    height: 48px;
    background: rgba(11,11,20,0.92);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    border-bottom: 1px solid rgba(255,255,255,0.06);
    display: flex; align-items: center;
    padding: 0 24px; gap: 16px;
  }
  .header-brand {
    font-weight: 700; font-size: 15px;
    background: linear-gradient(135deg, #60a5fa, #a78bfa);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  .header-sep { color: rgba(255,255,255,0.12); }
  .header-file { font-size: 13px; color: #888; font-family: "SF Mono","Fira Code",monospace; }
  .header-controls { margin-left: auto; display: flex; align-items: center; gap: 12px; }
  .header-legend { display: flex; gap: 16px; font-size: 11px; color: #666; margin-right: 16px; }
  .legend-dot {
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    margin-right: 5px; vertical-align: middle;
  }
  .toggle-btn {
    display: flex; align-items: center; gap: 6px;
    padding: 5px 14px; border-radius: 8px;
    border: 1px solid rgba(255,255,255,0.1);
    background: rgba(255,255,255,0.04);
    color: #aaa; font-size: 12px; font-weight: 600;
    cursor: pointer; transition: all 0.2s; font-family: inherit;
  }
  .toggle-btn:hover { background: rgba(255,255,255,0.08); color: #ddd; border-color: rgba(255,255,255,0.18); }
  .toggle-btn svg { width: 14px; height: 14px; }

  /* ---- viewport ---- */
  .viewport {
    position: absolute; top: 48px; left: 0; right: 0; bottom: 0;
    overflow: auto; cursor: grab;
  }
  .viewport:active { cursor: grabbing; }
  .canvas {
    position: relative;
    min-width: 100%; min-height: 100%;
    transition: width 0.5s, height 0.5s;
  }

  /* ---- edges ---- */
  .edges-layer { position: absolute; top: 0; left: 0; pointer-events: none; }
  .edge-path {
    fill: none; stroke: #7c6dd8; stroke-width: 2.5; opacity: 0.7;
    transition: opacity 0.25s, stroke-width 0.25s, stroke 0.25s;
  }
  .edge-path.highlighted {
    stroke: #a78bfa; stroke-width: 3.5; opacity: 1;
    filter: drop-shadow(0 0 4px rgba(167,139,250,0.5));
  }
  .edge-path.faded { opacity: 0.08; }

  /* ---- nodes ---- */
  .node {
    position: absolute; width: 200px; height: 64px;
    border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    gap: 10px; padding: 0 16px;
    cursor: grab;
    border: 1px solid rgba(255,255,255,0.06);
    user-select: none;
    transition: box-shadow 0.3s, border-color 0.3s;
    z-index: 2;
  }
  .node:active { cursor: grabbing; }
  .node.active { border-color: rgba(167,139,250,0.6); z-index: 3; }
  .node .icon { font-size: 18px; flex-shrink: 0; line-height: 1; }
  .node .label {
    font-size: 13px; font-weight: 600; line-height: 1.3;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    font-family: "SF Mono","Fira Code","Cascadia Code",monospace;
  }
  .node .dep-count {
    position: absolute; top: -8px; right: -8px;
    min-width: 20px; height: 20px; padding: 0 5px;
    border-radius: 10px; font-size: 10px; font-weight: 700;
    display: flex; align-items: center; justify-content: center;
    border: 2px solid #0b0b14; opacity: 0; transition: opacity 0.2s;
  }
  .node:hover .dep-count { opacity: 1; }

  .node.source {
    background: linear-gradient(135deg, rgba(16,185,129,0.12), rgba(16,185,129,0.04));
    border-color: rgba(16,185,129,0.2);
  }
  .node.source:hover { box-shadow: 0 8px 32px rgba(16,185,129,0.15), 0 0 0 1px rgba(16,185,129,0.3); }
  .node.source .label { color: #6ee7b7; }
  .node.source .dep-count { background: #065f46; color: #6ee7b7; }

  .node.cte {
    background: linear-gradient(135deg, rgba(96,165,250,0.12), rgba(139,92,246,0.08));
    border-color: rgba(96,165,250,0.2);
  }
  .node.cte:hover { box-shadow: 0 8px 32px rgba(96,165,250,0.15), 0 0 0 1px rgba(96,165,250,0.3); }
  .node.cte .label { color: #93c5fd; }
  .node.cte .dep-count { background: #1e3a5f; color: #93c5fd; }

  .node.final {
    background: linear-gradient(135deg, rgba(251,146,60,0.14), rgba(251,146,60,0.04));
    border-color: rgba(251,146,60,0.25);
  }
  .node.final:hover { box-shadow: 0 8px 32px rgba(251,146,60,0.18), 0 0 0 1px rgba(251,146,60,0.35); }
  .node.final .label { color: #fdba74; }
  .node.final .dep-count { background: #7c2d12; color: #fdba74; }

  .node.faded { opacity: 0.18; pointer-events: none; }

  /* ---- detail panel ---- */
  .panel-overlay {
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.4);
    z-index: 200; opacity: 0; pointer-events: none;
    transition: opacity 0.3s;
  }
  .panel-overlay.open { opacity: 1; pointer-events: auto; }
  .panel {
    position: fixed; top: 0; right: 0; bottom: 0;
    width: min(620px, 92vw);
    z-index: 210; background: #111119;
    border-left: 1px solid rgba(255,255,255,0.06);
    transform: translateX(100%);
    transition: transform 0.35s cubic-bezier(0.16,1,0.3,1);
    display: flex; flex-direction: column;
    box-shadow: -24px 0 80px rgba(0,0,0,0.5);
    overflow: hidden;
  }
  .panel.open { transform: translateX(0); }

  /* panel header */
  .panel-header {
    padding: 20px 24px 16px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    display: flex; align-items: center; gap: 12px;
    flex-shrink: 0;
  }
  .type-badge {
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; padding: 3px 8px; border-radius: 6px;
  }
  .type-badge.source { background: rgba(16,185,129,0.15); color: #6ee7b7; }
  .type-badge.cte { background: rgba(96,165,250,0.15); color: #93c5fd; }
  .type-badge.final { background: rgba(251,146,60,0.15); color: #fdba74; }
  .panel-header .node-name {
    font-size: 16px; font-weight: 700;
    font-family: "SF Mono","Fira Code",monospace; color: #f0f0f0;
  }
  .panel-close {
    margin-left: auto;
    background: rgba(255,255,255,0.06);
    border: none; color: #888; font-size: 18px;
    width: 32px; height: 32px; border-radius: 8px;
    cursor: pointer; display: flex; align-items: center; justify-content: center;
    transition: background 0.15s, color 0.15s;
  }
  .panel-close:hover { background: rgba(255,255,255,0.12); color: #eee; }

  /* panel scrollable body */
  .panel-body { flex: 1; overflow-y: auto; overflow-x: hidden; }

  /* panel sections */
  .p-section {
    padding: 16px 24px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
  }
  .p-section:last-child { border-bottom: none; }
  .p-title {
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; color: #555; margin-bottom: 8px;
  }

  /* tags row */
  .tag-row { display: flex; flex-wrap: wrap; gap: 6px; }
  .tag {
    font-size: 10px; font-weight: 700; letter-spacing: 0.04em;
    padding: 3px 9px; border-radius: 6px;
    text-transform: uppercase;
  }
  .tag.transform {
    background: rgba(139,92,246,0.12); color: #c4b5fd;
    border: 1px solid rgba(139,92,246,0.2);
  }
  .tag.complexity-simple { background: rgba(16,185,129,0.12); color: #6ee7b7; border: 1px solid rgba(16,185,129,0.18); }
  .tag.complexity-moderate { background: rgba(251,191,36,0.12); color: #fcd34d; border: 1px solid rgba(251,191,36,0.18); }
  .tag.complexity-complex { background: rgba(251,146,60,0.12); color: #fdba74; border: 1px solid rgba(251,146,60,0.18); }
  .tag.complexity-very-complex { background: rgba(239,68,68,0.12); color: #fca5a5; border: 1px solid rgba(239,68,68,0.18); }

  /* graph context row */
  .graph-ctx { display: flex; gap: 20px; }
  .graph-ctx-item {
    display: flex; flex-direction: column; gap: 2px;
  }
  .graph-ctx-num {
    font-size: 22px; font-weight: 700; color: #e0e0e0;
    font-family: "SF Mono","Fira Code",monospace;
  }
  .graph-ctx-label { font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: 0.06em; }

  /* dep/output tags */
  .dep-tag, .col-tag {
    display: inline-block; padding: 3px 10px; margin: 3px 4px 3px 0;
    border-radius: 6px; font-size: 12px;
    font-family: "SF Mono","Fira Code",monospace;
    background: rgba(255,255,255,0.04); color: #aaa;
    border: 1px solid rgba(255,255,255,0.06);
  }
  .col-tag { font-size: 11px; color: #888; }

  /* columns per source */
  .source-group { margin-bottom: 10px; }
  .source-group:last-child { margin-bottom: 0; }
  .source-group-name {
    font-size: 12px; font-weight: 600; color: #93c5fd;
    font-family: "SF Mono","Fira Code",monospace;
    margin-bottom: 4px;
  }

  /* join cards */
  .join-card {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px; padding: 10px 14px; margin-bottom: 8px;
  }
  .join-card:last-child { margin-bottom: 0; }
  .join-type {
    font-size: 10px; font-weight: 700; color: #c084fc;
    text-transform: uppercase; letter-spacing: 0.06em;
  }
  .join-table {
    font-size: 13px; font-weight: 600; color: #e0e0e0;
    font-family: "SF Mono","Fira Code",monospace;
    margin: 4px 0;
  }
  .join-on {
    font-size: 11px; color: #888;
    font-family: "SF Mono","Fira Code",monospace;
  }

  /* window fn cards */
  .win-card {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px; padding: 10px 14px; margin-bottom: 8px;
  }
  .win-card:last-child { margin-bottom: 0; }
  .win-fn {
    font-size: 12px; font-weight: 600; color: #67e8f9;
    font-family: "SF Mono","Fira Code",monospace;
  }
  .win-detail {
    font-size: 11px; color: #888; margin-top: 3px;
    font-family: "SF Mono","Fira Code",monospace;
  }

  /* filter/having/group blocks */
  .clause-block {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px; padding: 10px 14px;
    font-family: "SF Mono","Fira Code",monospace;
    font-size: 12px; line-height: 1.6; color: #c9d1d9;
    white-space: pre-wrap; word-break: break-word;
  }

  /* complexity stats */
  .complexity-grid {
    display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;
  }
  .complexity-stat {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.04);
    border-radius: 8px; padding: 8px 12px;
    text-align: center;
  }
  .complexity-stat-num {
    font-size: 18px; font-weight: 700; color: #e0e0e0;
    font-family: "SF Mono","Fira Code",monospace;
  }
  .complexity-stat-label { font-size: 9px; color: #666; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }

  /* SQL section */
  .sql-block {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px; padding: 16px 20px;
    font-family: "SF Mono","Fira Code","Cascadia Code",monospace;
    font-size: 12.5px; line-height: 1.65; color: #c9d1d9;
    white-space: pre-wrap; word-break: break-word; tab-size: 2;
  }
  .sql-block .kw { color: #c084fc; font-weight: 600; }
  .sql-block .fn { color: #67e8f9; }
  .sql-block .str { color: #86efac; }
  .sql-block .num { color: #fdba74; }
  .sql-block .cmt { color: #4b5563; font-style: italic; }

  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }
</style>
</head>
<body>

<div class="header">
  <span class="header-brand">CTE Flow</span>
  <span class="header-sep">|</span>
  <span class="header-file">%%FILENAME%%</span>
  <div class="header-controls">
    <div class="header-legend">
      <span><span class="legend-dot" style="background:#10b981;"></span>Source</span>
      <span><span class="legend-dot" style="background:#60a5fa;"></span>CTE</span>
      <span><span class="legend-dot" style="background:#fb923c;"></span>Final</span>
    </div>
    <button class="toggle-btn" id="toggleDir" title="Toggle layout direction">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
           stroke-linecap="round" stroke-linejoin="round" id="toggleIcon">
        <path d="M12 3v18M12 3l-4 4M12 3l4 4"/>
      </svg>
      <span id="toggleLabel">Top-Down</span>
    </button>
  </div>
</div>

<div class="viewport" id="viewport">
  <div class="canvas" id="canvas">
    <svg class="edges-layer" id="edgesSvg">
      <defs>
        <marker id="ah" markerWidth="12" markerHeight="10"
                refX="11" refY="5" orient="auto" markerUnits="userSpaceOnUse">
          <polygon points="0 0, 12 5, 0 10" fill="#a78bfa" opacity="0.85"/>
        </marker>
        <marker id="ahHi" markerWidth="14" markerHeight="11"
                refX="13" refY="5.5" orient="auto" markerUnits="userSpaceOnUse">
          <polygon points="0 0, 14 5.5, 0 11" fill="#c4b5fd"/>
        </marker>
      </defs>
    </svg>
    <div id="nodesContainer"></div>
  </div>
</div>

<div class="panel-overlay" id="panelOverlay"></div>
<div class="panel" id="panel">
  <div class="panel-header">
    <span class="type-badge" id="panelBadge"></span>
    <span class="node-name" id="panelName"></span>
    <button class="panel-close" id="panelClose">&times;</button>
  </div>
  <div class="panel-body" id="panelBody"></div>
</div>

<script>
const GRAPH = %%GRAPH_JSON%%;
const NODE_W = 200, NODE_H = 64, PAD = 80;
const LAYER_GAP_TB = 120, NODE_GAP_TB = 60;
const LAYER_GAP_LR = 280, NODE_GAP_LR = 80;
let direction = 'TB', nodePos = {}, dragging = null, activeNodeId = null;

// ===================== LAYOUT =====================
function topoSort() {
  const inDeg = {}, children = {};
  GRAPH.nodes.forEach(n => { inDeg[n.id] = 0; children[n.id] = []; });
  GRAPH.edges.forEach(e => { inDeg[e.to]++; children[e.from].push(e.to); });
  const queue = [];
  GRAPH.nodes.forEach(n => { if (inDeg[n.id] === 0) queue.push(n.id); });
  const order = [], layerOf = {};
  queue.forEach(id => { layerOf[id] = 0; });
  let head = 0;
  while (head < queue.length) {
    const nid = queue[head++];
    order.push(nid);
    children[nid].forEach(ch => {
      layerOf[ch] = Math.max(layerOf[ch] || 0, layerOf[nid] + 1);
      if (--inDeg[ch] === 0) queue.push(ch);
    });
  }
  GRAPH.nodes.forEach(n => { if (!(n.id in layerOf)) { layerOf[n.id] = 0; order.push(n.id); }});
  return { order, layerOf };
}
function computeLayout(dir) {
  const { order, layerOf } = topoSort();
  const layers = {};
  order.forEach(id => { const L = layerOf[id]; if (!layers[L]) layers[L] = []; layers[L].push(id); });
  const numLayers = Math.max(...Object.keys(layers).map(Number)) + 1;
  const pos = {};
  if (dir === 'TB') {
    let maxC = 0;
    for (let l = 0; l < numLayers; l++) maxC = Math.max(maxC, (layers[l]||[]).length);
    for (let l = 0; l < numLayers; l++) {
      const m = layers[l] || [];
      const tw = m.length * NODE_W + (m.length - 1) * NODE_GAP_TB;
      const mw = maxC * NODE_W + (maxC - 1) * NODE_GAP_TB;
      const sx = PAD + (mw - tw) / 2;
      const y = PAD + l * (NODE_H + LAYER_GAP_TB);
      m.forEach((id, i) => { pos[id] = { x: sx + i * (NODE_W + NODE_GAP_TB), y }; });
    }
  } else {
    let maxC = 0;
    for (let l = 0; l < numLayers; l++) maxC = Math.max(maxC, (layers[l]||[]).length);
    for (let l = 0; l < numLayers; l++) {
      const m = layers[l] || [];
      const th = m.length * NODE_H + (m.length - 1) * NODE_GAP_LR;
      const mh = maxC * NODE_H + (maxC - 1) * NODE_GAP_LR;
      const sy = PAD + (mh - th) / 2;
      const x = PAD + l * (NODE_W + LAYER_GAP_LR);
      m.forEach((id, i) => { pos[id] = { x, y: sy + i * (NODE_H + NODE_GAP_LR) }; });
    }
  }
  return pos;
}
function canvasSize() {
  let mx = 0, my = 0;
  Object.values(nodePos).forEach(p => { if (p.x+NODE_W>mx) mx=p.x+NODE_W; if (p.y+NODE_H>my) my=p.y+NODE_H; });
  return { w: mx + PAD, h: my + PAD };
}

// ===================== RENDERING =====================
const canvas = document.getElementById('canvas');
const edgesSvg = document.getElementById('edgesSvg');
const nodesContainer = document.getElementById('nodesContainer');
const icons = {
  source: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#6ee7b7" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/><path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/></svg>',
  cte: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#93c5fd" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="M9 3v18"/><path d="M3 9h18"/></svg>',
  final: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#fdba74" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M8 12l3 3 5-5"/></svg>',
};
function renderNodes() {
  nodesContainer.innerHTML = '';
  const depCount = {};
  GRAPH.nodes.forEach(n => { depCount[n.id] = 0; });
  GRAPH.edges.forEach(e => { depCount[e.to]++; });
  GRAPH.nodes.forEach(n => {
    const p = nodePos[n.id], el = document.createElement('div');
    el.className = 'node ' + n.type;
    el.dataset.id = n.id;
    el.style.left = p.x + 'px'; el.style.top = p.y + 'px';
    const badge = depCount[n.id] > 0 ? '<span class="dep-count">'+depCount[n.id]+'</span>' : '';
    el.innerHTML = badge + '<span class="icon">'+icons[n.type]+'</span><span class="label" title="'+esc(n.label)+'">'+esc(n.label)+'</span>';
    el.addEventListener('pointerdown', onNodePointerDown);
    el.addEventListener('click', () => { if (!el._wasDragged) openPanel(n.id); });
    el.addEventListener('mouseenter', () => { if (!activeNodeId && !dragging) highlightConnected(n.id); });
    el.addEventListener('mouseleave', () => { if (!activeNodeId && !dragging) clearHighlight(); });
    nodesContainer.appendChild(el);
  });
}
function renderEdges() {
  edgesSvg.querySelectorAll('path').forEach(p => p.remove());
  const sz = canvasSize();
  edgesSvg.setAttribute('width', sz.w); edgesSvg.setAttribute('height', sz.h);
  GRAPH.edges.forEach((e, i) => {
    const from = nodePos[e.from], to = nodePos[e.to];
    if (!from || !to) return;
    let x1,y1,x2,y2;
    if (direction==='TB') { x1=from.x+NODE_W/2; y1=from.y+NODE_H; x2=to.x+NODE_W/2; y2=to.y; }
    else { x1=from.x+NODE_W; y1=from.y+NODE_H/2; x2=to.x; y2=to.y+NODE_H/2; }
    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('d', bezier(x1,y1,x2,y2));
    path.setAttribute('class','edge-path');
    path.setAttribute('data-idx', i);
    path.setAttribute('marker-end','url(#ah)');
    edgesSvg.appendChild(path);
  });
}
function bezier(x1,y1,x2,y2) {
  if (direction==='TB') { const d=Math.abs(y2-y1)*0.45; return `M ${x1} ${y1} C ${x1} ${y1+d}, ${x2} ${y2-d}, ${x2} ${y2}`; }
  else { const d=Math.abs(x2-x1)*0.45; return `M ${x1} ${y1} C ${x1+d} ${y1}, ${x2-d} ${y2}, ${x2} ${y2}`; }
}
function updateCanvasSize() { const s=canvasSize(); canvas.style.width=s.w+'px'; canvas.style.height=s.h+'px'; }
function fullRender() { updateCanvasSize(); renderNodes(); renderEdges(); }

// ===================== DRAGGING =====================
function canvasCoords(e) {
  // Convert a pointer event to canvas-relative coordinates,
  // accounting for viewport scroll and the fixed header.
  const vp = document.getElementById('viewport');
  return {
    x: e.clientX + vp.scrollLeft - vp.getBoundingClientRect().left,
    y: e.clientY + vp.scrollTop  - vp.getBoundingClientRect().top
  };
}
function onNodePointerDown(e) {
  if (e.button!==0) return;
  const el=e.currentTarget, id=el.dataset.id, p=nodePos[id];
  const c = canvasCoords(e);
  el._wasDragged=false;
  dragging={id, offsetX: c.x - p.x, offsetY: c.y - p.y, el};
  el.setPointerCapture(e.pointerId);
  el.addEventListener('pointermove',onNodePointerMove);
  el.addEventListener('pointerup',onNodePointerUp);
  e.stopPropagation();
}
function onNodePointerMove(e) {
  if(!dragging) return;
  const c = canvasCoords(e);
  const nx = c.x - dragging.offsetX;
  const ny = c.y - dragging.offsetY;
  nodePos[dragging.id]={x:nx,y:ny};
  dragging.el.style.left=nx+'px'; dragging.el.style.top=ny+'px';
  dragging.el._wasDragged=true;
  renderEdges(); updateCanvasSize();
}
function onNodePointerUp(e) {
  if(!dragging) return;
  const el=dragging.el;
  el.releasePointerCapture(e.pointerId);
  el.removeEventListener('pointermove',onNodePointerMove);
  el.removeEventListener('pointerup',onNodePointerUp);
  if(el._wasDragged) setTimeout(()=>{el._wasDragged=false;},0);
  dragging=null;
}

// ===================== TOGGLE =====================
document.getElementById('toggleDir').addEventListener('click', () => {
  direction = direction==='TB' ? 'LR' : 'TB';
  nodePos = computeLayout(direction);
  document.getElementById('toggleLabel').textContent = direction==='TB' ? 'Top-Down' : 'Left-Right';
  document.getElementById('toggleIcon').innerHTML = direction==='TB'
    ? '<path d="M12 3v18M12 3l-4 4M12 3l4 4"/>' : '<path d="M3 12h18M3 12l4-4M3 12l4 4"/>';
  document.querySelectorAll('.node').forEach(el => {
    const p = nodePos[el.dataset.id];
    if (p) {
      el.style.transition='left 0.45s cubic-bezier(0.16,1,0.3,1), top 0.45s cubic-bezier(0.16,1,0.3,1)';
      el.style.left=p.x+'px'; el.style.top=p.y+'px';
    }
  });
  updateCanvasSize();
  setTimeout(() => { renderEdges(); document.querySelectorAll('.node').forEach(el=>{el.style.transition='';}); }, 460);
});

// ===================== HIGHLIGHT =====================
function highlightConnected(nodeId) {
  const conn = new Set([nodeId]), ce = new Set();
  function up(n) { GRAPH.edges.forEach((e,i)=>{ if(e.to===n){ce.add(i); if(!conn.has(e.from)){conn.add(e.from);up(e.from);}} }); }
  function dn(n) { GRAPH.edges.forEach((e,i)=>{ if(e.from===n){ce.add(i); if(!conn.has(e.to)){conn.add(e.to);dn(e.to);}} }); }
  up(nodeId); dn(nodeId);
  document.querySelectorAll('.node').forEach(el=>{ el.classList.toggle('faded',!conn.has(el.dataset.id)); });
  document.querySelectorAll('.edge-path').forEach(el=>{ const h=ce.has(+el.dataset.idx); el.classList.toggle('highlighted',h); el.classList.toggle('faded',!h); el.setAttribute('marker-end',h?'url(#ahHi)':'url(#ah)'); });
}
function clearHighlight() {
  document.querySelectorAll('.node').forEach(n=>n.classList.remove('faded'));
  document.querySelectorAll('.edge-path').forEach(el=>{ el.classList.remove('highlighted','faded'); el.setAttribute('marker-end','url(#ah)'); });
}

// ===================== PANEL =====================
const panelOverlay = document.getElementById('panelOverlay');
const panel = document.getElementById('panel');
const panelBody = document.getElementById('panelBody');

function openPanel(nodeId) {
  const node = GRAPH.nodes.find(n => n.id === nodeId);
  if (!node) return;
  activeNodeId = nodeId;
  const m = node.meta || {};

  document.getElementById('panelBadge').className = 'type-badge ' + node.type;
  document.getElementById('panelBadge').textContent =
    node.type==='source' ? 'Source Table' : node.type==='cte' ? 'CTE' : 'Final SELECT';
  document.getElementById('panelName').textContent = node.label;

  let html = '';

  // --- Graph context (upstream / downstream) ---
  const upstream = new Set(), downstream = new Set();
  function walkUp(n) { GRAPH.edges.forEach(e=>{ if(e.to===n && !upstream.has(e.from)){upstream.add(e.from);walkUp(e.from);} }); }
  function walkDn(n) { GRAPH.edges.forEach(e=>{ if(e.from===n && !downstream.has(e.to)){downstream.add(e.to);walkDn(e.to);} }); }
  walkUp(nodeId); walkDn(nodeId);
  const directIn = GRAPH.edges.filter(e => e.to === nodeId).length;
  const directOut = GRAPH.edges.filter(e => e.from === nodeId).length;

  html += '<div class="p-section"><div class="graph-ctx">'
    + ctxItem(directIn, 'Direct Inputs')
    + ctxItem(upstream.size, 'Total Upstream')
    + ctxItem(directOut, 'Direct Outputs')
    + ctxItem(downstream.size, 'Total Downstream')
    + '</div></div>';

  // --- Transformation tags + complexity ---
  const tags = m.tags || [];
  const cpx = m.complexity;
  if (tags.length || cpx) {
    html += '<div class="p-section">';
    if (tags.length) {
      html += '<div class="p-title">Transformations</div><div class="tag-row" style="margin-bottom:10px">';
      tags.forEach(t => { html += '<span class="tag transform">'+esc(t)+'</span>'; });
      html += '</div>';
    }
    if (cpx) {
      html += '<div class="p-title">Complexity</div><div class="tag-row">';
      const cls = 'complexity-' + cpx.level.replace(' ','-');
      html += '<span class="tag '+cls+'">'+esc(cpx.level)+'</span>';
      html += '</div>';
    }
    html += '</div>';
  }

  // --- Dependencies ---
  const deps = node.deps || [];
  if (deps.length) {
    html += '<div class="p-section"><div class="p-title">Dependencies</div>'
      + deps.map(d => '<span class="dep-tag">'+esc(d)+'</span>').join('') + '</div>';
  }

  // --- Used by (source tables) ---
  const usedBy = m.used_by || [];
  if (usedBy.length) {
    html += '<div class="p-section"><div class="p-title">Used By</div>'
      + usedBy.map(d => '<span class="dep-tag">'+esc(d)+'</span>').join('') + '</div>';
  }

  // --- Output columns ---
  const outCols = m.output_columns || [];
  if (outCols.length) {
    html += '<div class="p-section"><div class="p-title">Output Columns <span style="color:#555;font-weight:400">(' + outCols.length + ')</span></div>'
      + outCols.map(c => '<span class="col-tag">'+esc(c)+'</span>').join('') + '</div>';
  }

  // --- Columns referenced (source tables) ---
  const colsRef = m.columns_referenced || [];
  if (colsRef.length) {
    html += '<div class="p-section"><div class="p-title">Columns Referenced <span style="color:#555;font-weight:400">(' + colsRef.length + ')</span></div>'
      + colsRef.map(c => '<span class="col-tag">'+esc(c)+'</span>').join('') + '</div>';
  }

  // --- Joins ---
  const joins = m.joins || [];
  if (joins.length) {
    html += '<div class="p-section"><div class="p-title">Joins <span style="color:#555;font-weight:400">(' + joins.length + ')</span></div>';
    joins.forEach(j => {
      html += '<div class="join-card">'
        + '<div class="join-type">'+esc(j.type)+'</div>'
        + '<div class="join-table">'+esc(j.table)+(j.alias?' <span style="color:#666">as</span> '+esc(j.alias):'')+'</div>'
        + (j.on ? '<div class="join-on">ON '+esc(j.on)+'</div>' : '')
        + '</div>';
    });
    html += '</div>';
  }

  // --- WHERE ---
  if (m.where) {
    html += '<div class="p-section"><div class="p-title">Where Clause</div>'
      + '<div class="clause-block">'+esc(m.where)+'</div></div>';
  }

  // --- HAVING ---
  if (m.having) {
    html += '<div class="p-section"><div class="p-title">Having Clause</div>'
      + '<div class="clause-block">'+esc(m.having)+'</div></div>';
  }

  // --- GROUP BY ---
  const groupBy = m.group_by || [];
  if (groupBy.length) {
    html += '<div class="p-section"><div class="p-title">Group By</div>'
      + groupBy.map(g => '<span class="col-tag">'+esc(g)+'</span>').join('') + '</div>';
  }

  // --- Window functions ---
  const wins = m.window_functions || [];
  if (wins.length) {
    html += '<div class="p-section"><div class="p-title">Window Functions <span style="color:#555;font-weight:400">(' + wins.length + ')</span></div>';
    wins.forEach(w => {
      html += '<div class="win-card"><div class="win-fn">'+esc(w.function)+'</div>';
      if (w.partition_by.length) html += '<div class="win-detail">PARTITION BY '+esc(w.partition_by.join(', '))+'</div>';
      if (w.order_by.length) html += '<div class="win-detail">ORDER BY '+esc(w.order_by.join(', '))+'</div>';
      html += '</div>';
    });
    html += '</div>';
  }

  // --- Columns per source ---
  const colsPerSrc = m.columns_per_source || {};
  const srcKeys = Object.keys(colsPerSrc).filter(k => colsPerSrc[k].length > 0);
  if (srcKeys.length) {
    html += '<div class="p-section"><div class="p-title">Columns Referenced Per Source</div>';
    srcKeys.forEach(src => {
      html += '<div class="source-group"><div class="source-group-name">'+esc(src)+'</div>'
        + colsPerSrc[src].map(c => '<span class="col-tag">'+esc(c)+'</span>').join('')
        + '</div>';
    });
    html += '</div>';
  }

  // --- Complexity breakdown ---
  if (cpx) {
    html += '<div class="p-section"><div class="p-title">Complexity Breakdown</div>'
      + '<div class="complexity-grid">'
      + cStat(cpx.lines, 'Lines')
      + cStat(cpx.joins, 'Joins')
      + cStat(cpx.window_functions, 'Windows')
      + cStat(cpx.case_statements, 'CASE')
      + cStat(cpx.subqueries, 'Subqueries')
      + cStat(cpx.expressions, 'AST Nodes')
      + '</div></div>';
  }

  // --- Full SQL ---
  html += '<div class="p-section"><div class="p-title">Full SQL</div>'
    + '<div class="sql-block">'+highlightSQL(node.sql)+'</div></div>';

  panelBody.innerHTML = html;

  highlightConnected(nodeId);
  document.querySelectorAll('.node').forEach(el => el.classList.remove('active'));
  const ae = document.querySelector('.node[data-id="'+CSS.escape(nodeId)+'"]');
  if (ae) ae.classList.add('active');
  panel.classList.add('open');
  panelOverlay.classList.add('open');
}

function ctxItem(num, label) {
  return '<div class="graph-ctx-item"><span class="graph-ctx-num">'+num+'</span><span class="graph-ctx-label">'+label+'</span></div>';
}
function cStat(num, label) {
  return '<div class="complexity-stat"><div class="complexity-stat-num">'+num+'</div><div class="complexity-stat-label">'+label+'</div></div>';
}

function closePanel() {
  panel.classList.remove('open'); panelOverlay.classList.remove('open');
  document.querySelectorAll('.node').forEach(el=>{el.classList.remove('active','faded');});
  clearHighlight(); activeNodeId=null;
}
panelOverlay.addEventListener('click', closePanel);
document.getElementById('panelClose').addEventListener('click', closePanel);
document.addEventListener('keydown', e => { if (e.key==='Escape') closePanel(); });

// ===================== SQL HIGHLIGHTING =====================
function highlightSQL(sql) {
  let s = sql.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  s = s.replace(/(--[^\n]*)/g, '<span class="cmt">$1</span>');
  s = s.replace(/('(?:[^'\\]|\\.)*')/g, '<span class="str">$1</span>');
  s = s.replace(/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>');
  const kws = ['SELECT','FROM','WHERE','JOIN','LEFT','RIGHT','INNER','OUTER','FULL',
    'CROSS','ON','AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','AS','WITH',
    'GROUP\\s+BY','ORDER\\s+BY','HAVING','LIMIT','OFFSET','UNION','ALL',
    'INSERT','UPDATE','DELETE','CREATE','ALTER','DROP','INTO','VALUES','SET',
    'CASE','WHEN','THEN','ELSE','END','DISTINCT','IS','NULL','TRUE','FALSE',
    'ASC','DESC','OVER','PARTITION\\s+BY','ROWS','RANGE','RECURSIVE','LATERAL',
    'FILTER','QUALIFY','WINDOW','EXCEPT','INTERSECT','FETCH','NEXT','ONLY','CAST',
    'COALESCE','NULLIF','ANY','SOME','TABLE','VIEW','INDEX','PRIMARY','KEY',
    'FOREIGN','REFERENCES','CONSTRAINT','CHECK','DEFAULT','UNIQUE','IF','REPLACE'];
  const kwPat = new RegExp('\\b(' + kws.join('|') + ')\\b', 'gi');
  s = s.replace(kwPat, m => '<span class="kw">' + m.toUpperCase() + '</span>');
  s = s.replace(/\b([A-Z_][A-Z0-9_]*)\s*(?=\()/gi, (m, fn) => {
    if (kws.some(k => new RegExp('^'+k+'$','i').test(fn))) return m;
    return '<span class="fn">' + fn + '</span>';
  });
  return s;
}
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// ===================== INIT =====================
nodePos = computeLayout(direction);
fullRender();
requestAnimationFrame(() => {
  const vp = document.getElementById('viewport'), sz = canvasSize();
  if (sz.w > vp.clientWidth) vp.scrollLeft = (sz.w - vp.clientWidth) / 2;
  if (sz.h > vp.clientHeight) vp.scrollTop = Math.max(0, (sz.h - vp.clientHeight) / 4);
});
</script>
</body>
</html>"""


def generate_html(graph: dict, filename: str) -> str:
    graph_json = json.dumps(graph)
    out = _HTML_TEMPLATE
    out = out.replace("%%FILENAME%%", html_mod.escape(filename))
    out = out.replace("%%GRAPH_JSON%%", graph_json)
    return out


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m cteflow.renderer <graph.json> [-o output.html]")
        sys.exit(1)

    json_file = Path(sys.argv[1])
    if not json_file.exists():
        print(f"Error: file not found: {json_file}")
        sys.exit(1)

    output_path = None
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "-o" and i + 1 < len(args):
            output_path = args[i + 1]
            i += 2
        else:
            i += 1

    if output_path is None:
        output_path = json_file.stem.replace("_graph", "") + "_flow.html"

    graph = json.loads(json_file.read_text())
    html = generate_html(graph, json_file.stem)
    Path(output_path).write_text(html)
    print(f"HTML written to {output_path}")


if __name__ == "__main__":
    main()
