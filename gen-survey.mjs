import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { writeFileSync } from 'fs';

// Parcel coordinates (lon, lat) → local feet
const coords = [
  [-74.413379, 40.461908],  // 0 - NW corner (street side)
  [-74.413006, 40.46161],   // 1 - NE corner (street side)
  [-74.413141, 40.461454],  // 2 - SE corner (rear)
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

const pageW = 792;
const pageH = 612;
const margin = 80;
const titleH = 90;
const drawW = pageW - margin * 2;
const drawH = pageH - margin - titleH - 70;

const sc = Math.min(drawW / maxX, drawH / maxY) * 0.80;
const offsetX = margin + (drawW - maxX * sc) / 2;
const offsetY = margin + 50 + (drawH - maxY * sc) / 2;

function toP(p) { return { x: offsetX + p.x * sc, y: offsetY + p.y * sc }; }

const pdfDoc = await PDFDocument.create();
const page = pdfDoc.addPage([pageW, pageH]);
const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
const fontB = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

const black = rgb(0, 0, 0);
const blue = rgb(0, 0.2, 0.7);
const red = rgb(0.8, 0, 0);
const green = rgb(0, 0.5, 0);
const gray = rgb(0.5, 0.5, 0.5);
const lightBlue = rgb(0.7, 0.85, 1);
const lightGray = rgb(0.92, 0.92, 0.92);
const dkGray = rgb(0.35, 0.35, 0.35);

// ── Title block ──
page.drawRectangle({ x: margin - 5, y: pageH - titleH - 5, width: pageW - margin * 2 + 10, height: titleH, borderColor: black, borderWidth: 1 });
page.drawText('PLOT PLAN \u2014 ABOVE-GROUND POOL INSTALLATION', {
  x: margin + 80, y: pageH - 28, size: 14, font: fontB, color: black });
page.drawText('56 Eggers St, East Brunswick, NJ 08816', {
  x: margin + 130, y: pageH - 46, size: 11, font, color: black });
page.drawText('Block: 602   Lot: 6   Zone: R3   Lot Area: 16,117 sq ft (0.37 ac)   Bi-Level 2AG, Built 1963', {
  x: margin + 30, y: pageH - 62, size: 9, font, color: dkGray });
page.drawText('Owner: Yamil D. Velez   |   Contractor: DR ZEE POOL INSTALLATION LLC   |   Date: March 2026', {
  x: margin + 50, y: pageH - 78, size: 9, font, color: dkGray });

// ── Property boundary ──
for (let i = 0; i < pts.length - 1; i++) {
  const a = toP(pts[i]);
  const b = toP(pts[i + 1]);
  page.drawLine({ start: a, end: b, thickness: 2.5, color: black });

  const dx = pts[i + 1].x - pts[i].x;
  const dy = pts[i + 1].y - pts[i].y;
  const len = Math.sqrt(dx * dx + dy * dy);
  const mx = (a.x + b.x) / 2;
  const my = (a.y + b.y) / 2;
  const nx = -dy / len * 14;
  const ny = dx / len * 14;

  page.drawText(len.toFixed(1) + "'", {
    x: mx + nx - 12, y: my + ny - 4, size: 8, font: fontB, color: dkGray });
}

// Corner markers
for (let i = 0; i < pts.length - 1; i++) {
  const p = toP(pts[i]);
  page.drawCircle({ x: p.x, y: p.y, size: 3, color: black });
  page.drawText('IP', { x: p.x + 4, y: p.y + 2, size: 6, font, color: gray });
}

// ── Street label along north edge (pts 0 → 1) ──
const st0 = toP(pts[0]);
const st1 = toP(pts[1]);
page.drawText('E G G E R S   S T R E E T', {
  x: (st0.x + st1.x) / 2 - 55, y: (st0.y + st1.y) / 2 + 18, size: 10, font: fontB, color: dkGray });

// ── Adjacent properties ──
page.drawText('54 Eggers St', {
  x: Math.min(toP(pts[4]).x, toP(pts[0]).x) - 62, y: (toP(pts[4]).y + toP(pts[0]).y) / 2 - 4, size: 7, font, color: gray });
page.drawText('53 University Rd', {
  x: Math.max(toP(pts[1]).x, toP(pts[2]).x) + 8, y: (toP(pts[1]).y + toP(pts[2]).y) / 2 - 4, size: 7, font, color: gray });
page.drawText('25 Marlow Rd', {
  x: toP(pts[3]).x - 30, y: toP(pts[3]).y - 16, size: 7, font, color: gray });

// ── House — near north (street) side, setback ~30ft ──
const streetMidX = (pts[0].x + pts[1].x) / 2;
const streetMidY = (pts[0].y + pts[1].y) / 2;
const houseCx = streetMidX;
const houseCy = streetMidY - 40;
const houseWft = 42;
const houseHft = 30;
const houseW = houseWft * sc;
const houseH = houseHft * sc;
const houseP = toP({ x: houseCx, y: houseCy });

page.drawRectangle({
  x: houseP.x - houseW / 2, y: houseP.y - houseH / 2,
  width: houseW, height: houseH,
  borderColor: black, borderWidth: 1.5, color: lightGray,
});
page.drawText('HOUSE', {
  x: houseP.x - 16, y: houseP.y + 4, size: 10, font: fontB, color: black });
page.drawText("42' x 30' (Bi-Level)", {
  x: houseP.x - 32, y: houseP.y - 10, size: 7, font, color: dkGray });

// ── Driveway ──
const driveX = houseP.x - houseW / 2 - 10 * sc;
const driveTopY = toP({ x: 0, y: streetMidY }).y;
page.drawRectangle({
  x: driveX, y: houseP.y - houseH / 2,
  width: 10 * sc, height: driveTopY - (houseP.y - houseH / 2),
  borderColor: gray, borderWidth: 0.5, color: rgb(0.85, 0.85, 0.85),
});
page.drawText('DRVWY', {
  x: driveX + 2, y: (driveTopY + houseP.y) / 2, size: 6, font, color: gray });

// ── POOL — rear yard, centered behind house ──
const poolCx = houseCx;
const poolCy = houseCy - 50;
const poolR = 9;
const poolP = toP({ x: poolCx, y: poolCy });
const poolRpx = poolR * sc;

page.drawCircle({
  x: poolP.x, y: poolP.y, size: poolRpx,
  borderColor: blue, borderWidth: 2.5, color: lightBlue,
});
page.drawText('POOL', {
  x: poolP.x - 12, y: poolP.y + 5, size: 10, font: fontB, color: blue });
page.drawText("18' round", {
  x: poolP.x - 16, y: poolP.y - 8, size: 8, font, color: blue });
page.drawText('above-ground', {
  x: poolP.x - 22, y: poolP.y - 18, size: 7, font, color: blue });
page.drawText('Meranti', {
  x: poolP.x - 14, y: poolP.y - 27, size: 7, font, color: blue });

// ── Filter/Pump ──
const filterP = toP({ x: poolCx + poolR + 6, y: poolCy });
const fw = 5 * sc;
const fh = 4 * sc;
page.drawRectangle({
  x: filterP.x, y: filterP.y - fh / 2,
  width: fw, height: fh,
  borderColor: green, borderWidth: 1.5, color: rgb(0.85, 1, 0.85),
});
page.drawText('FILTER/', {
  x: filterP.x - 2, y: filterP.y + fh / 2 + 6, size: 6, font: fontB, color: green });
page.drawText('PUMP', {
  x: filterP.x, y: filterP.y + fh / 2 + 0, size: 6, font: fontB, color: green });

// ── Setback annotation ──
page.drawText('> 10\' setback', {
  x: poolP.x - 50, y: poolP.y - poolRpx - 14, size: 7, font, color: red });

// ── North arrow ──
const naX = pageW - margin + 15;
const naY = pageH - titleH - 30;
page.drawText('N', { x: naX - 4, y: naY + 5, size: 12, font: fontB, color: black });
page.drawLine({ start: { x: naX, y: naY }, end: { x: naX, y: naY - 35 }, thickness: 1.5, color: black });
page.drawLine({ start: { x: naX - 5, y: naY - 5 }, end: { x: naX, y: naY + 2 }, thickness: 1.5, color: black });
page.drawLine({ start: { x: naX + 5, y: naY - 5 }, end: { x: naX, y: naY + 2 }, thickness: 1.5, color: black });

// ── Scale bar ──
const scaleBarFt = 50;
const scaleBarPx = scaleBarFt * sc;
const sbX = margin;
const sbY = margin + 15;
page.drawLine({ start: { x: sbX, y: sbY }, end: { x: sbX + scaleBarPx, y: sbY }, thickness: 1.5, color: black });
page.drawLine({ start: { x: sbX, y: sbY - 3 }, end: { x: sbX, y: sbY + 3 }, thickness: 1, color: black });
page.drawLine({ start: { x: sbX + scaleBarPx, y: sbY - 3 }, end: { x: sbX + scaleBarPx, y: sbY + 3 }, thickness: 1, color: black });
page.drawText('0', { x: sbX - 2, y: sbY + 5, size: 7, font, color: black });
page.drawText(scaleBarFt + "'", { x: sbX + scaleBarPx - 5, y: sbY + 5, size: 7, font, color: black });
page.drawText('SCALE', { x: sbX + scaleBarPx / 2 - 12, y: sbY + 5, size: 7, font: fontB, color: black });

// ── Legend ──
const legX = pageW - margin - 140;
const legY = margin + 25;
page.drawRectangle({ x: legX - 5, y: legY - 10, width: 150, height: 75, borderColor: black, borderWidth: 0.5 });
page.drawText('LEGEND', { x: legX + 45, y: legY + 52, size: 8, font: fontB, color: black });
page.drawLine({ start: { x: legX, y: legY + 42 }, end: { x: legX + 20, y: legY + 42 }, thickness: 2.5, color: black });
page.drawText('Property line', { x: legX + 25, y: legY + 39, size: 7, font, color: black });
page.drawRectangle({ x: legX + 2, y: legY + 27, width: 16, height: 10, borderColor: black, borderWidth: 1, color: lightGray });
page.drawText('Building footprint', { x: legX + 25, y: legY + 29, size: 7, font, color: black });
page.drawCircle({ x: legX + 10, y: legY + 20, size: 6, borderColor: blue, borderWidth: 1.5, color: lightBlue });
page.drawText("Pool (18' round AG)", { x: legX + 25, y: legY + 17, size: 7, font, color: black });
page.drawRectangle({ x: legX + 4, y: legY + 4, width: 12, height: 8, borderColor: green, borderWidth: 1, color: rgb(0.85, 1, 0.85) });
page.drawText('Filter/Pump equip.', { x: legX + 25, y: legY + 5, size: 7, font, color: black });

// ── Setback notes ──
page.drawText('SETBACK REQUIREMENTS (East Brunswick Township):', {
  x: margin, y: margin - 5, size: 7, font: fontB, color: red });
page.drawText("Pool & equipment: 10 ft min from all property lines  |  30 ft from street line  |  AG pool 4ft+ height: no fence if ladder removed when not in use", {
  x: margin, y: margin - 16, size: 7, font, color: red });

// ── Footer ──
page.drawText('Hand-marked plot plan per East Brunswick Twp. above-ground pool permit requirements (Section 9).', {
  x: margin, y: 14, size: 6, font, color: gray });
page.drawText('Pool location approximate. Final placement confirmed on-site by DR ZEE POOL INSTALLATION LLC. Parcel geometry from NJ GIS/Tax Map data.', {
  x: margin, y: 6, size: 6, font, color: gray });

const out = await pdfDoc.save();
writeFileSync('C:/project/yamil-browser/pool-survey-marked.pdf', out);
console.log('Survey saved:', out.length, 'bytes');
