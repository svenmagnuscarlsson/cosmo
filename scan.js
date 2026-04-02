import { readdir, stat, writeFile, mkdir } from 'node:fs/promises'
import { join, extname, basename, relative } from 'node:path'

// ── Config ──────────────────────────────────────────────────────────
const ROOT = process.argv[2] || '/Users/andersbj/Projekt'
const MAX_DEPTH = Number(process.argv[3]) || 3
const OUT = 'data/filesystem.json'

const SKIP = new Set([
  'node_modules', '.git', '.next', '.nuxt', '__pycache__', '.venv', 'venv',
  'dist', 'build', '.cache', '.parcel-cache', '.turbo', 'coverage',
  '.DS_Store', 'Thumbs.db', '.idea', '.vscode', '.expo', '.gradle',
  'vendor', 'target', 'obj', 'bin', 'pods', 'Pods', 'DerivedData',
  '.terraform', '.angular', 'bower_components', 'jspm_packages',
  'typings', '.sass-cache', 'tmp', 'temp', 'logs',
  '.output', '.vercel', '.netlify', '.serverless', '.amplify',
  'android', 'ios', 'windows', 'macos', 'linux', 'web',
])

// ── Extension → category color mapping ──────────────────────────────
const EXT_COLORS = {
  // Code
  '.js':   '#f7df1e', '.jsx':  '#61dafb', '.ts':   '#3178c6', '.tsx':  '#61dafb',
  '.py':   '#3572a5', '.rb':   '#cc342d', '.go':   '#00add8', '.rs':   '#dea584',
  '.java': '#b07219', '.c':    '#555555', '.cpp':  '#f34b7d', '.h':    '#555555',
  '.swift':'#f05138', '.kt':   '#a97bff',
  // Web
  '.html': '#e34c26', '.css':  '#563d7c', '.scss': '#c6538c', '.vue':  '#42b883',
  '.svelte':'#ff3e00',
  // Data / Config
  '.json': '#a8d08d', '.yaml': '#cb171e', '.yml':  '#cb171e', '.toml': '#9c4221',
  '.xml':  '#0060ac', '.csv':  '#217346', '.sql':  '#e38c00',
  // Docs
  '.md':   '#ffffff', '.txt':  '#cccccc', '.pdf':  '#ff4444', '.doc':  '#2b579a',
  // Media
  '.png':  '#ff69b4', '.jpg':  '#ff69b4', '.jpeg': '#ff69b4', '.gif':  '#ff69b4',
  '.svg':  '#ffb13b', '.mp3':  '#1db954', '.mp4':  '#9b59b6', '.wav':  '#1db954',
  // Shell / DevOps
  '.sh':   '#89e051', '.bash': '#89e051', '.zsh':  '#89e051',
  '.dockerfile': '#384d54', '.env': '#ecd53f',
}

const DIR_COLOR = '#4fc3f7'       // directories
const DEFAULT_COLOR = '#888888'   // unknown extensions

function colorFor(name, isDir) {
  if (isDir) return DIR_COLOR
  const ext = extname(name).toLowerCase()
  return EXT_COLORS[ext] || DEFAULT_COLOR
}

// ── Category for clustering ─────────────────────────────────────────
function categoryFor(name, isDir) {
  if (isDir) return 'directory'
  const ext = extname(name).toLowerCase()
  if (['.js','.jsx','.ts','.tsx','.py','.rb','.go','.rs','.java','.c','.cpp','.h','.swift','.kt'].includes(ext)) return 'code'
  if (['.html','.css','.scss','.vue','.svelte'].includes(ext)) return 'web'
  if (['.json','.yaml','.yml','.toml','.xml','.csv','.sql','.env'].includes(ext)) return 'data'
  if (['.md','.txt','.pdf','.doc','.docx'].includes(ext)) return 'docs'
  if (['.png','.jpg','.jpeg','.gif','.svg','.mp3','.mp4','.wav','.webm','.webp'].includes(ext)) return 'media'
  if (['.sh','.bash','.zsh'].includes(ext)) return 'shell'
  return 'other'
}

// ── Walk ────────────────────────────────────────────────────────────
const points = []
const links = []
let fileCount = 0

async function walk(dir, depth) {
  const id = relative(ROOT, dir) || '.'
  const name = basename(dir)
  const info = await stat(dir)

  points.push({
    id,
    label: name,
    color: colorFor(name, true),
    category: 'directory',
    size: 0,       // will be updated later based on child count
    fileSize: 0,
    depth,
    isDir: true,
  })

  if (depth >= MAX_DEPTH) return

  let entries
  try {
    entries = await readdir(dir, { withFileTypes: true })
  } catch {
    return // permission denied etc.
  }

  let childCount = 0

  for (const entry of entries) {
    if (SKIP.has(entry.name)) continue
    if (entry.name.startsWith('.') && entry.name !== '.env') continue

    const fullPath = join(dir, entry.name)
    const childId = relative(ROOT, fullPath)

    if (entry.isDirectory()) {
      links.push({ source: id, target: childId })
      childCount++
      await walk(fullPath, depth + 1)
    } else if (entry.isFile()) {
      let fileSize = 0
      try {
        const fstat = await stat(fullPath)
        fileSize = fstat.size
      } catch { /* skip */ }

      points.push({
        id: childId,
        label: entry.name,
        color: colorFor(entry.name, false),
        category: categoryFor(entry.name, false),
        size: Math.max(1, Math.log2(fileSize + 1)),
        fileSize,
        depth: depth + 1,
        isDir: false,
      })
      links.push({ source: id, target: childId })
      childCount++
      fileCount++
    }
  }

  // Update directory node size based on children
  const dirPoint = points.find(p => p.id === id)
  if (dirPoint) {
    dirPoint.size = Math.max(3, Math.sqrt(childCount) * 2)
  }
}

// ── Main ────────────────────────────────────────────────────────────
console.log(`Scanning ${ROOT} (max depth ${MAX_DEPTH})...`)
const t0 = Date.now()

await walk(ROOT, 0)

// Add numeric indices and build id→index map for links
const idToIndex = new Map()
for (let i = 0; i < points.length; i++) {
  points[i].index = i
  idToIndex.set(points[i].id, i)
}
for (const link of links) {
  link.sourceIndex = idToIndex.get(link.source)
  link.targetIndex = idToIndex.get(link.target)
}

const data = { root: ROOT, points, links }
await mkdir('data', { recursive: true })
await writeFile(OUT, JSON.stringify(data))

const elapsed = ((Date.now() - t0) / 1000).toFixed(1)
console.log(`Done in ${elapsed}s — ${points.length} nodes, ${links.length} edges, ${fileCount} files`)
console.log(`Wrote ${OUT}`)
