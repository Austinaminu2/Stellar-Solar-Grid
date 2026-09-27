/**
 * Dependency-free single-page PDF builder shared by receipts and bills (#902).
 *
 * Produces a minimal PDF 1.4 document with a standards-compliant xref table.
 * Each line is rendered in Helvetica; a line may override its font size and
 * whether it is bold, and an empty string renders as a blank spacer line.
 */

export type PdfLine = string | { text: string; size?: number; bold?: boolean };

function escapePdfText(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

export function createTextPdf(lines: PdfLine[], options: { fontSize?: number; lineHeight?: number } = {}): Buffer {
  const defaultSize = options.fontSize ?? 14;
  const lineHeight = options.lineHeight ?? 24;
  const ops: string[] = ["BT", "72 740 Td"];
  lines.forEach((line, i) => {
    const { text, size, bold } = typeof line === "string" ? { text: line } : line;
    const font = bold ? "/F2" : "/F1";
    ops.push(`${font} ${size ?? defaultSize} Tf`);
    ops.push(i === 0 ? `(${escapePdfText(text)}) Tj` : `0 -${lineHeight} Td (${escapePdfText(text)}) Tj`);
  });
  ops.push("ET");
  const stream = ops.join("\n");

  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> /Contents 4 0 R >>",
    `<< /Length ${Buffer.byteLength(stream, "utf8")} >>\nstream\n${stream}\nendstream`,
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
  ];
  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  for (let i = 0; i < objects.length; i++) {
    offsets.push(Buffer.byteLength(pdf, "utf8"));
    pdf += `${i + 1} 0 obj\n${objects[i]}\nendobj\n`;
  }
  const xrefOffset = Buffer.byteLength(pdf, "utf8");
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (let i = 1; i < offsets.length; i++) pdf += `${String(offsets[i]).padStart(10, "0")} 00000 n \n`;
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`;
  return Buffer.from(pdf, "utf8");
}
