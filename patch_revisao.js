import fs from 'fs';

const path = './client/src/pages/RevisaoFinalProdutos.tsx';
let code = fs.readFileSync(path, 'utf8');

// Replace text-slate-300 with text-slate-700 dark:text-slate-200 to improve visibility in light mode and keep it good in dark mode, or make it brighter in dark mode with text-slate-200
code = code.replace(/text-slate-300/g, 'text-slate-700 dark:text-slate-200');

// Header background: from bg-slate-950/90 to bg-slate-100/90 dark:bg-slate-900/90
code = code.replace(/bg-slate-950\/95/g, 'bg-slate-100/95 dark:bg-slate-900/95');
code = code.replace(/bg-slate-950\/90/g, 'bg-slate-100/90 dark:bg-slate-900/90');

// Header text: text-slate-200 to text-slate-700 dark:text-slate-200
code = code.replace(/text-slate-200/g, 'text-slate-700 dark:text-slate-200');

// Amber background: make it more visible in dark mode
code = code.replace(/bg-amber-500\/10/g, 'bg-amber-500/10 dark:bg-amber-400/10');

// Muted text in lists: make it a bit brighter in dark mode
code = code.replace(/text-muted-foreground/g, 'text-slate-600 dark:text-slate-400');

// Change text-slate-500 to text-slate-500 dark:text-slate-400
code = code.replace(/text-slate-500/g, 'text-slate-500 dark:text-slate-400');

fs.writeFileSync(path, code);
console.log("Patched");
