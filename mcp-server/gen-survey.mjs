import { writeFileSync } from 'fs';

// Parcel coordinates (lon, lat) → local feet
const coords = [
  [-74.413379, 40.461908],  // 0 - NW corner (street side)
  [-74.413006, 40.46161],   // 1 - NE corner (street side)
  [-74.413141, 40.461454],  // 2 - SE corner (rear, east side)
  [-74.41321,  40.461375],  // 3 - S corner (rear, deepest)
  [-74.413639, 40.461718],  // 4 - W corner
  [-74.413379, 40.461908],  // close polygon
];

const LAT_FT = 364000;
const LON_FT = 278000;
const minLon = Math.min(...coords.map(c => c[0]));
const minLat = Math.min(...coords.map(c => c[1]));
const pts = coords.map(([lon, lat]) => ({
  x: (lon - minLon) * LON_FT,
  y: (lat - minLat) * LAT_FT,
}));

const maxX = Math.max(...pts.map(p => p.x));
const maxY = Math.max(...pts.map(p => p.y));

// SVG canvas — US Letter landscape (11" x 8.5" at 96dpi)
const svgW = 1056;
const svgH = 816;
const margin = 40;
const titleH = 90;
const bottomArea = 85;
const drawW = svgW - margin * 2;
const drawH = svgH - titleH - bottomArea;

const sc = Math.min(drawW / maxX, drawH / maxY) * 0.95;
// Center the plot in the draw area
const offsetX = margin + (drawW - maxX * sc) / 2;
const offsetY = titleH + (drawH - maxY * sc) / 2;

// SVG Y is top-down, lot Y is bottom-up → flip
function toP(p) {
  return { x: offsetX + p.x * sc, y: offsetY + (maxY - p.y) * sc };
}

// Street edge direction: pts 4 → 0 (the ~100ft edge along Eggers St)
const streetDx = pts[0].x - pts[4].x;
const streetDy = pts[0].y - pts[4].y;
const streetLen = Math.sqrt(streetDx * streetDx + streetDy * streetDy);
const uPar = { x: streetDx / streetLen, y: streetDy / streetLen };
const uPerp = { x: -uPar.y, y: uPar.x };
const centroid = {
  x: pts.slice(0, -1).reduce((s, p) => s + p.x, 0) / (pts.length - 1),
  y: pts.slice(0, -1).reduce((s, p) => s + p.y, 0) / (pts.length - 1),
};
const streetMid = { x: (pts[4].x + pts[0].x) / 2, y: (pts[4].y + pts[0].y) / 2 };
const toCentroid = { x: centroid.x - streetMid.x, y: centroid.y - streetMid.y };
const dot = toCentroid.x * uPerp.x + toCentroid.y * uPerp.y;
if (dot < 0) { uPerp.x = -uPerp.x; uPerp.y = -uPerp.y; }

console.log('Street angle:', Math.atan2(streetDy, streetDx) * 180 / Math.PI, 'deg');
console.log('uPerp (into lot):', uPerp);

// ── Build SVG ──
const els = [];
const el = (tag, attrs, content = '') => {
  const a = Object.entries(attrs).map(([k, v]) => `${k}="${v}"`).join(' ');
  return content ? `<${tag} ${a}>${content}</${tag}>` : `<${tag} ${a}/>`;
};

// ── Title block ──
els.push(el('rect', { x: margin - 5, y: 8, width: svgW - margin * 2 + 10, height: titleH - 10, fill: 'white', stroke: 'black', 'stroke-width': 1 }));
els.push(el('text', { x: svgW / 2, y: 32, 'text-anchor': 'middle', 'font-size': 16, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'PLOT PLAN &#x2014; ABOVE-GROUND POOL INSTALLATION'));
els.push(el('text', { x: svgW / 2, y: 50, 'text-anchor': 'middle', 'font-size': 12, 'font-family': 'Helvetica, Arial, sans-serif' }, '56 Eggers St, East Brunswick, NJ 08816'));
els.push(el('text', { x: svgW / 2, y: 66, 'text-anchor': 'middle', 'font-size': 11, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#333' }, 'Block: 602  Lot: 6  Zone: R3  Lot Area: 16,117 sq ft (0.37 ac)  Bi-Level 2AG, Built 1963'));
els.push(el('text', { x: svgW / 2, y: 82, 'text-anchor': 'middle', 'font-size': 11, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#333' }, 'Owner: Yamil D. Velez  |  Contractor: DR ZEE POOL INSTALLATION LLC  |  Date: March 2026'));

// ── Property boundary ──
const boundaryPoints = pts.slice(0, -1).map(p => toP(p));
const polyStr = boundaryPoints.map(p => `${p.x},${p.y}`).join(' ');
els.push(el('polygon', { points: polyStr, fill: 'none', stroke: 'black', 'stroke-width': 3, 'stroke-linejoin': 'round' }));

// Edge lengths
for (let i = 0; i < pts.length - 1; i++) {
  const a = toP(pts[i]);
  const b = toP(pts[i + 1]);
  const dx = pts[i + 1].x - pts[i].x;
  const dy = pts[i + 1].y - pts[i].y;
  const len = Math.sqrt(dx * dx + dy * dy);
  const mx = (a.x + b.x) / 2;
  const my = (a.y + b.y) / 2;
  // Offset outward from lot center
  const cx = (boundaryPoints.reduce((s, p) => s + p.x, 0)) / boundaryPoints.length;
  const cy = (boundaryPoints.reduce((s, p) => s + p.y, 0)) / boundaryPoints.length;
  const nx = mx - cx;
  const ny = my - cy;
  const nl = Math.sqrt(nx * nx + ny * ny);
  const ox = nx / nl * 18;
  const oy = ny / nl * 18;
  els.push(el('text', { x: mx + ox, y: my + oy, 'text-anchor': 'middle', 'font-size': 9, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#555' }, `${len.toFixed(1)}'`));
}

// Corner markers (IP)
for (let i = 0; i < pts.length - 1; i++) {
  const p = toP(pts[i]);
  els.push(el('circle', { cx: p.x, cy: p.y, r: 4, fill: 'black' }));
  els.push(el('text', { x: p.x + 8, y: p.y - 4, 'font-size': 7, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#888' }, 'IP'));
}

// ── Street label — along edge 4→0 (Eggers St), offset OUTSIDE the lot ──
const st0 = toP(pts[4]);
const st1 = toP(pts[0]);
const stMx = (st0.x + st1.x) / 2;
const stMy = (st0.y + st1.y) / 2;
const streetAngleDeg = Math.atan2(st1.y - st0.y, st1.x - st0.x) * 180 / Math.PI;
// Offset away from lot center (toward street)
const lotCx = boundaryPoints.reduce((s, p) => s + p.x, 0) / boundaryPoints.length;
const lotCy = boundaryPoints.reduce((s, p) => s + p.y, 0) / boundaryPoints.length;
const awayX = stMx - lotCx;
const awayY = stMy - lotCy;
const awayLen = Math.sqrt(awayX * awayX + awayY * awayY);
const stLabelX = stMx + (awayX / awayLen) * 60;
const stLabelY = stMy + (awayY / awayLen) * 60;
els.push(el('text', {
  x: stLabelX, y: stLabelY,
  'text-anchor': 'middle', 'font-size': 14, 'font-weight': 'bold',
  'font-family': 'Helvetica, Arial, sans-serif', fill: '#444',
  'letter-spacing': '4',
  transform: `rotate(${streetAngleDeg}, ${stLabelX}, ${stLabelY})`
}, 'EGGERS STREET'));

// ── Adjacent properties ──
// Left neighbor (edge 3→4, toward Marlow/west)
const w0 = toP(pts[3]);
const w1 = toP(pts[4]);
els.push(el('text', { x: Math.min(w0.x, w1.x) - 10, y: (w0.y + w1.y) / 2, 'text-anchor': 'end', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#888' }, '54 Eggers St'));
// Right neighbor (edge 0→1, toward University)
const e0 = toP(pts[0]);
const e1 = toP(pts[1]);
els.push(el('text', { x: Math.max(e0.x, e1.x) + 12, y: (e0.y + e1.y) / 2, 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#888' }, '53 University Rd'));
// Rear (edge 2→3)
const r0 = toP(pts[2]);
const r1 = toP(pts[3]);
els.push(el('text', { x: (r0.x + r1.x) / 2, y: Math.max(r0.y, r1.y) + 18, 'text-anchor': 'middle', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#888' }, '25 Marlow Rd'));

// ── HOUSE — close to street, wide side faces Eggers St ──
const houseSetback = 28; // very close to street (satellite shows ~25-30ft)
const houseShiftRight = 0; // centered on street frontage
const houseCx = streetMid.x + uPerp.x * houseSetback + uPar.x * houseShiftRight;
const houseCy = streetMid.y + uPerp.y * houseSetback + uPar.y * houseShiftRight;
const houseW = 42; // 42ft wide (along street — wide face toward Eggers)
const houseD = 30; // 30ft deep (into lot)
const hP = toP({ x: houseCx, y: houseCy });
const hWpx = houseW * sc;
const hDpx = houseD * sc;

// Compute rotation angle to align house with street
const houseRotDeg = streetAngleDeg;
els.push(`<g transform="rotate(${houseRotDeg}, ${hP.x}, ${hP.y})">`);
els.push(el('rect', {
  x: hP.x - hWpx / 2, y: hP.y - hDpx / 2,
  width: hWpx, height: hDpx,
  fill: '#ddd', stroke: 'black', 'stroke-width': 1.5,
}));
// Labels (not rotated — we'll close the group first and add them separately)
els.push('</g>');
// House labels at center (horizontal, no rotation)
els.push(el('text', { x: hP.x, y: hP.y - 2, 'text-anchor': 'middle', 'font-size': 12, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'HOUSE'));
els.push(el('text', { x: hP.x, y: hP.y + 12, 'text-anchor': 'middle', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#555' }, `42' x 30'`));
els.push(el('text', { x: hP.x, y: hP.y + 22, 'text-anchor': 'middle', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#555' }, '(Bi-Level)'));

// Front door indicator — street-facing edge of house
// The street side of the house is toward pt0→pt1 (away from uPerp)
const frontCx = houseCx - uPerp.x * houseD / 2;
const frontCy = houseCy - uPerp.y * houseD / 2;
const fP = toP({ x: frontCx, y: frontCy });
els.push(el('text', { x: fP.x - 25, y: fP.y - 4, 'font-size': 7, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#555' }, 'FRONT'));
els.push(el('line', { x1: fP.x - 8, y1: fP.y, x2: fP.x + 8, y2: fP.y, stroke: '#555', 'stroke-width': 2 }));

// ── GARAGE — attached to right/east side, rotated with house ──
const garageW = 20;
const garageD = 20;
const gWpx = garageW * sc;
const gDpx = garageD * sc;
// Position garage center: shift right along street from house center, align front
const garageOffsetAlongStreet = (houseW / 2 + garageW / 2);
const gCx = houseCx + garageOffsetAlongStreet * uPar.x;
const gCy = houseCy + garageOffsetAlongStreet * uPar.y;
const gP = toP({ x: gCx, y: gCy });
els.push(`<g transform="rotate(${houseRotDeg}, ${gP.x}, ${gP.y})">`);
els.push(el('rect', {
  x: gP.x - gWpx / 2, y: gP.y - gDpx / 2,
  width: gWpx, height: gDpx,
  fill: '#ddd', stroke: 'black', 'stroke-width': 1.5,
}));
els.push('</g>');
els.push(el('text', { x: gP.x, y: gP.y, 'text-anchor': 'middle', 'font-size': 9, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'GARAGE'));
els.push(el('text', { x: gP.x, y: gP.y + 12, 'text-anchor': 'middle', 'font-size': 7, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#555' }, `20' x 20'`));

// ── Driveway ──
const driveOffsetAlongStreet = (houseW / 2 + 8);
const driveFrontCx = houseCx + driveOffsetAlongStreet * uPar.x - houseSetback * 0.5 * uPerp.x;
const driveFrontCy = houseCy + driveOffsetAlongStreet * uPar.y - houseSetback * 0.5 * uPerp.y;
const driveBackCx = houseCx + driveOffsetAlongStreet * uPar.x;
const driveBackCy = houseCy + driveOffsetAlongStreet * uPar.y;
const dF = toP({ x: driveFrontCx, y: driveFrontCy });
const dB = toP({ x: driveBackCx, y: driveBackCy });
els.push(el('line', { x1: dF.x - 5, y1: dF.y, x2: dB.x - 5, y2: dB.y, stroke: '#999', 'stroke-width': 0.5 }));
els.push(el('line', { x1: dF.x + 15, y1: dF.y, x2: dB.x + 15, y2: dB.y, stroke: '#999', 'stroke-width': 0.5 }));
const dMx = (dF.x + dB.x) / 2 + 5;
const dMy = (dF.y + dB.y) / 2;
els.push(el('text', { x: dMx, y: dMy, 'text-anchor': 'middle', 'font-size': 6, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#999' }, 'DRVWY'));

// ── POOL — behind house, left side of yard ──
const poolSetback = 75;
const poolShiftLeft = 12;
const poolCx = streetMid.x + uPerp.x * poolSetback - uPar.x * poolShiftLeft;
const poolCy = streetMid.y + uPerp.y * poolSetback - uPar.y * poolShiftLeft;
const poolR = 9;
const poolP = toP({ x: poolCx, y: poolCy });
const poolRpx = poolR * sc;

els.push(el('circle', { cx: poolP.x, cy: poolP.y, r: poolRpx, fill: '#b3d4ff', stroke: '#0033b3', 'stroke-width': 2.5 }));
els.push(el('text', { x: poolP.x, y: poolP.y - 2, 'text-anchor': 'middle', 'font-size': 11, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#0033b3' }, 'POOL'));
els.push(el('text', { x: poolP.x, y: poolP.y + 10, 'text-anchor': 'middle', 'font-size': 9, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#0033b3' }, `18' round`));
// Below circle
els.push(el('text', { x: poolP.x, y: poolP.y + poolRpx + 12, 'text-anchor': 'middle', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#0033b3' }, 'above-ground'));
els.push(el('text', { x: poolP.x, y: poolP.y + poolRpx + 22, 'text-anchor': 'middle', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#0033b3' }, 'Meranti'));

// ── Filter/Pump — below pool ──
const fwPx = 12;
const fhPx = 10;
const filterPx = poolP.x;
const filterPy = poolP.y + poolRpx + 36;
els.push(el('rect', { x: filterPx - fwPx / 2, y: filterPy - fhPx / 2, width: fwPx, height: fhPx, fill: '#d9ffd9', stroke: '#008000', 'stroke-width': 1.5 }));
els.push(el('text', { x: filterPx, y: filterPy + fhPx / 2 + 10, 'text-anchor': 'middle', 'font-size': 7, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#008000' }, 'FILTER/PUMP'));

// ── Setback annotation — left of pool ──
const setbackX = poolP.x - poolRpx - 15;
const setbackY = poolP.y - 8;
els.push(el('text', { x: setbackX, y: setbackY, 'text-anchor': 'end', 'font-size': 8, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#cc0000' }, `> 10' setback`));
els.push(el('text', { x: setbackX, y: setbackY + 11, 'text-anchor': 'end', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#cc0000' }, 'from all'));
els.push(el('text', { x: setbackX, y: setbackY + 22, 'text-anchor': 'end', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#cc0000' }, 'property lines'));

// ── North arrow ──
const naX = svgW - margin - 30;
const naY = titleH + 25;
els.push(el('text', { x: naX, y: naY, 'text-anchor': 'middle', 'font-size': 14, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'N'));
els.push(el('line', { x1: naX, y1: naY + 5, x2: naX, y2: naY + 40, stroke: 'black', 'stroke-width': 1.5 }));
els.push(el('polygon', { points: `${naX},${naY + 3} ${naX - 5},${naY + 12} ${naX + 5},${naY + 12}`, fill: 'black' }));

// ── Scale bar ──
const scaleBarFt = 50;
const scaleBarPx = scaleBarFt * sc;
const sbX = margin;
const sbY = svgH - 80;
els.push(el('line', { x1: sbX, y1: sbY, x2: sbX + scaleBarPx, y2: sbY, stroke: 'black', 'stroke-width': 2 }));
els.push(el('line', { x1: sbX, y1: sbY - 5, x2: sbX, y2: sbY + 5, stroke: 'black', 'stroke-width': 1.5 }));
els.push(el('line', { x1: sbX + scaleBarPx, y1: sbY - 5, x2: sbX + scaleBarPx, y2: sbY + 5, stroke: 'black', 'stroke-width': 1.5 }));
els.push(el('text', { x: sbX, y: sbY - 9, 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif' }, '0'));
els.push(el('text', { x: sbX + scaleBarPx / 2, y: sbY - 9, 'text-anchor': 'middle', 'font-size': 9, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'SCALE'));
els.push(el('text', { x: sbX + scaleBarPx, y: sbY - 9, 'text-anchor': 'end', 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif' }, `${scaleBarFt}'`));

// ── Legend ──
const legX = svgW - margin - 195;
const legY = svgH - 95;
els.push(el('rect', { x: legX, y: legY, width: 185, height: 95, fill: 'white', stroke: 'black', 'stroke-width': 0.5 }));
els.push(el('text', { x: legX + 92, y: legY + 16, 'text-anchor': 'middle', 'font-size': 10, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif' }, 'LEGEND'));
els.push(el('line', { x1: legX + 12, y1: legY + 32, x2: legX + 32, y2: legY + 32, stroke: 'black', 'stroke-width': 3 }));
els.push(el('text', { x: legX + 40, y: legY + 35, 'font-size': 9, 'font-family': 'Helvetica, Arial, sans-serif' }, 'Property line'));
els.push(el('rect', { x: legX + 14, y: legY + 43, width: 16, height: 12, fill: '#ddd', stroke: 'black', 'stroke-width': 1 }));
els.push(el('text', { x: legX + 40, y: legY + 53, 'font-size': 9, 'font-family': 'Helvetica, Arial, sans-serif' }, 'Building footprint'));
els.push(el('circle', { cx: legX + 22, cy: legY + 67, r: 8, fill: '#b3d4ff', stroke: '#0033b3', 'stroke-width': 1.5 }));
els.push(el('text', { x: legX + 40, y: legY + 70, 'font-size': 9, 'font-family': 'Helvetica, Arial, sans-serif' }, `Pool (18' round AG)`));
els.push(el('rect', { x: legX + 15, y: legY + 79, width: 14, height: 10, fill: '#d9ffd9', stroke: '#008000', 'stroke-width': 1 }));
els.push(el('text', { x: legX + 40, y: legY + 88, 'font-size': 9, 'font-family': 'Helvetica, Arial, sans-serif' }, 'Filter/Pump equip.'));

// ── Setback requirements ──
const reqY = svgH - 42;
els.push(el('text', { x: margin, y: reqY, 'font-size': 9, 'font-weight': 'bold', 'font-family': 'Helvetica, Arial, sans-serif', fill: '#cc0000' }, 'SETBACK REQUIREMENTS (East Brunswick Township):'));
els.push(el('text', { x: margin, y: reqY + 14, 'font-size': 8, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#cc0000' }, 'Pool &amp; equipment: 10 ft min from all property lines  |  30 ft from street line  |  AG pool 4ft+ height: no fence if ladder removed when not in use'));

// ── Footer ──
els.push(el('text', { x: margin, y: svgH - 3, 'font-size': 7, 'font-family': 'Helvetica, Arial, sans-serif', fill: '#999' }, 'Hand-marked plot plan per East Brunswick Twp. above-ground pool permit requirements (Section 9). Pool location approximate.'));

// ── Assemble as HTML with embedded SVG for proper print layout ──
const svg = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Plot Plan - 56 Eggers St</title>
<style>
  @page {
    size: landscape;
    margin: 0.25in;
  }
  html, body {
    margin: 0;
    padding: 0;
    overflow: hidden;
  }
  svg {
    width: 100%;
    height: 100%;
    display: block;
  }
  @media print {
    html, body { width: 10.5in; height: 8in; }
    svg { width: 10.5in; height: 8in; }
  }
</style>
</head>
<body>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${svgW} ${svgH}" preserveAspectRatio="xMidYMid meet">
<rect width="100%" height="100%" fill="white"/>
${els.join('\n')}
</svg>
</body>
</html>`;

const outPath = 'C:/project/yamil-browser/pool-survey-marked.html';
writeFileSync(outPath, svg);
// Also save raw SVG for direct use
const svgOnly = svg.match(/<svg[\s\S]*<\/svg>/)[0];
writeFileSync('C:/project/yamil-browser/pool-survey-marked.svg', `<?xml version="1.0" encoding="UTF-8"?>\n${svgOnly}`);
console.log('Saved:', outPath, `(${svg.length} bytes)`);
