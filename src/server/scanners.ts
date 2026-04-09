/**
 * Content scanners for detecting portal-specific references in blog/page HTML.
 * Includes HubL template extraction for automated migration.
 */

export interface ContentWarning {
  type: "hubl" | "form_embed" | "cta_embed" | "broken_media";
  message: string;
  snippet: string;
  postId?: string;
}

export interface HubLReference {
  type: "module" | "include" | "widget" | "variable";
  path: string | null;
  raw: string;
  postId?: string;
}

// ── HubL extraction ──

// {% module "name" path="/path/to/module" %} or {% module "name" path="/path/to/module.module" %}
const MODULE_RE =
  /\{%\s*module\s+"[^"]*"\s+path\s*=\s*"([^"]+)"/gi;

// {% include "/path/to/template.html" %}
const INCLUDE_RE = /\{%\s*include\s+"([^"]+)"/gi;

// {% widget_block ... %} (legacy)
const WIDGET_RE = /\{%\s*widget_block\s+[^%]*%\}/gi;

// {{ content.xxx }} or {{ module.xxx }} — built-in variables, no extraction needed
const BUILTIN_VAR_RE = /\{\{\s*(?:content|request|blog_|local_|standard_|group)\./g;

const HUBL_BLOCK_RE = /\{%[^%]*%\}/g;
const HUBL_VAR_RE = /\{\{[^}]*\}\}/g;

/**
 * Extract all HubL references from HTML, categorized by type.
 * Returns actionable references (modules/includes to fetch from source).
 */
export function extractHubLReferences(
  html: string,
  postId?: string
): HubLReference[] {
  const refs: HubLReference[] = [];

  // Custom modules
  MODULE_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = MODULE_RE.exec(html)) !== null) {
    refs.push({
      type: "module",
      path: m[1]!,
      raw: m[0],
      postId,
    });
  }

  // Includes
  INCLUDE_RE.lastIndex = 0;
  while ((m = INCLUDE_RE.exec(html)) !== null) {
    refs.push({
      type: "include",
      path: m[1]!,
      raw: m[0],
      postId,
    });
  }

  // Legacy widgets (no extractable path, just flag)
  WIDGET_RE.lastIndex = 0;
  while ((m = WIDGET_RE.exec(html)) !== null) {
    refs.push({
      type: "widget",
      path: null,
      raw: m[0],
      postId,
    });
  }

  return refs;
}

/**
 * Get unique module/include paths from a set of HubL references.
 */
export function getUniquePaths(refs: HubLReference[]): {
  modulePaths: string[];
  includePaths: string[];
} {
  const modules = new Set<string>();
  const includes = new Set<string>();

  for (const ref of refs) {
    if (ref.type === "module" && ref.path) modules.add(ref.path);
    if (ref.type === "include" && ref.path) includes.add(ref.path);
  }

  return {
    modulePaths: Array.from(modules),
    includePaths: Array.from(includes),
  };
}

// ── Warning scanners (unchanged) ──

export function scanForHubL(html: string): string[] {
  const matches: string[] = [];
  let m2: RegExpExecArray | null;

  HUBL_BLOCK_RE.lastIndex = 0;
  while ((m2 = HUBL_BLOCK_RE.exec(html)) !== null) {
    // Skip built-in patterns that don't need migration
    if (/\{%\s*(?:if|elif|else|endif|for|endfor|set|block|endblock|extends|unless|endunless)\s/.test(m2[0])) {
      continue;
    }
    matches.push(m2[0]);
  }

  HUBL_VAR_RE.lastIndex = 0;
  while ((m2 = HUBL_VAR_RE.exec(html)) !== null) {
    // Skip CTA tokens — handled by scanForCtaEmbeds
    if (isCtaToken(m2[0])) continue;
    // Skip built-in HubSpot variables that work in any portal
    if (BUILTIN_VAR_RE.test(m2[0])) {
      BUILTIN_VAR_RE.lastIndex = 0;
      continue;
    }
    if (!m2[0].includes("\\{")) {
      matches.push(m2[0]);
    }
  }

  return matches;
}

export function scanForFormEmbeds(html: string): string[] {
  const FORM_EMBED_RE =
    /hbspt\.forms\.create\s*\(\s*\{[^}]*portalId\s*:\s*['"](\d+)['"]/g;
  const FORM_DATA_ATTR_RE = /data-form-id=['"]([^'"]+)['"]/g;
  const matches: string[] = [];
  let m2: RegExpExecArray | null;

  FORM_EMBED_RE.lastIndex = 0;
  while ((m2 = FORM_EMBED_RE.exec(html)) !== null) matches.push(m2[0]);
  FORM_DATA_ATTR_RE.lastIndex = 0;
  while ((m2 = FORM_DATA_ATTR_RE.exec(html)) !== null) matches.push(m2[0]);

  return matches;
}

export function scanForCtaEmbeds(html: string): string[] {
  const CTA_RE =
    /(?:cta_button|cta_simple|hs-cta-wrapper|data-cta-id)\s*[=(]['"]?([a-f0-9-]+)['"]?/gi;
  // Also catch HubL CTA function calls: {{cta('uuid')}} or {{ cta('uuid') }}
  const CTA_HUBL_RE = /\{\{\s*cta\s*\(\s*['"]([a-f0-9-]+)['"]\s*\)\s*\}\}/gi;
  const matches: string[] = [];
  let m2: RegExpExecArray | null;

  CTA_RE.lastIndex = 0;
  while ((m2 = CTA_RE.exec(html)) !== null) matches.push(m2[0]);
  CTA_HUBL_RE.lastIndex = 0;
  while ((m2 = CTA_HUBL_RE.exec(html)) !== null) matches.push(m2[0]);

  return matches;
}

/** Check if a {{...}} token is a CTA call */
function isCtaToken(token: string): boolean {
  return /\{\{\s*cta\s*\(/.test(token);
}

/**
 * Extract all unique CTA GUIDs from HTML.
 * Returns a map of GUID → list of post IDs that use it.
 */
export function extractCtaGuids(
  html: string,
  postId?: string
): Map<string, string[]> {
  const guids = new Map<string, string[]>();
  const CTA_GUID_RE = /\{\{\s*cta\s*\(\s*['"]([a-f0-9-]+)['"]\s*\)\s*\}\}/gi;
  let m: RegExpExecArray | null;
  CTA_GUID_RE.lastIndex = 0;
  while ((m = CTA_GUID_RE.exec(html)) !== null) {
    const guid = m[1]!;
    if (!guids.has(guid)) guids.set(guid, []);
    if (postId && !guids.get(guid)!.includes(postId)) {
      guids.get(guid)!.push(postId);
    }
  }
  // Also catch non-HubL CTA patterns
  const CTA_ATTR_GUID_RE = /data-cta-id=['"]([a-f0-9-]+)['"]/gi;
  CTA_ATTR_GUID_RE.lastIndex = 0;
  while ((m = CTA_ATTR_GUID_RE.exec(html)) !== null) {
    const guid = m[1]!;
    if (!guids.has(guid)) guids.set(guid, []);
    if (postId && !guids.get(guid)!.includes(postId)) {
      guids.get(guid)!.push(postId);
    }
  }
  return guids;
}

export function scanForBrokenMediaRefs(
  downloadResults: Map<string, boolean>
): string[] {
  const broken: string[] = [];
  for (const [url, success] of downloadResults) {
    if (!success) broken.push(url);
  }
  return broken;
}

export function scanContent(
  html: string,
  postId?: string,
  downloadResults?: Map<string, boolean>
): ContentWarning[] {
  const warnings: ContentWarning[] = [];

  for (const snippet of scanForHubL(html)) {
    warnings.push({
      type: "hubl",
      message:
        "HubL token found — template will be extracted and migrated automatically",
      snippet: snippet.slice(0, 120),
      postId,
    });
  }

  for (const snippet of scanForFormEmbeds(html)) {
    warnings.push({
      type: "form_embed",
      message:
        "Form embed with portal-specific ID — form must be recreated in target portal",
      snippet: snippet.slice(0, 120),
      postId,
    });
  }

  for (const snippet of scanForCtaEmbeds(html)) {
    warnings.push({
      type: "cta_embed",
      message:
        "CTA embed with portal-specific GUID — CTA must be recreated manually in target",
      snippet: snippet.slice(0, 120),
      postId,
    });
  }

  if (downloadResults) {
    for (const url of scanForBrokenMediaRefs(downloadResults)) {
      warnings.push({
        type: "broken_media",
        message:
          "Media URL returned 404 during export — image will be missing",
        snippet: url.slice(0, 120),
        postId,
      });
    }
  }

  return warnings;
}
