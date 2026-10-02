/* ================================================================
   תפריט נגישות — רכיב עצמאי, ללא תלויות, ללא צד שלישי.
   ================================================================

   (מקור: geniza-explorer/assets/a11y.js)

   שחרר לתוך כל אתר: <script src=".../a11y.js" defer></script>
   יחד עם a11y.css. אין תלות בספרייה, במסגרת עבודה או בשרת.

   למה לא תוסף נגישות מסחרי:
   1. הוא סקריפט שנטען מדומיין אחר ורץ בדף שלך — סתירה לכל מדיניות
      פרטיות שמבטיחה "אין צד שלישי".
   2. הוא לא עובד אופליין (רלוונטי ל-PWA).
   3. והעיקר: תפריט אינו מנגיש אתר. ת"י 5568 דורש נגישות בקוד עצמו.
      הרכיב הזה הוא תוספת נוחות מעל אתר שכבר נגיש, לא תחליף לו.

   אינטגרציה אופציונלית עם privacy.js: אם המשתנה הגלובלי PRIVACY קיים,
   כל קריאה/כתיבה לאחסון עוברת דרכו, כך שמי שביקש "בלי שמירה מקומית"
   לא מקבל כתיבה לדפדפן. בלעדיו הרכיב עובד מול localStorage ישירות.

   קיצור מקלדת: Alt+Shift+A. סגירה: Esc.
   ================================================================ */

const A11Y_KEY = 'a11y:v1';
const A11Y_FS = ['s', 'm', 'l'];
const A11Y_MODES = ['contrast', 'invert', 'mono'];
const A11Y_FLAGS = ['links', 'readable', 'spacing', 'still', 'cursor', 'focus'];

var A11Y = (function () {

  let state = { fs: 's', mode: '', links: 0, readable: 0, spacing: 0, still: 0, cursor: 0, focus: 0 };
  let panel = null, fab = null, lastFocus = null;

  /* ---------- אחסון ---------- */

  function readStore() {
    try {
      const raw = (typeof PRIVACY !== 'undefined')
        ? PRIVACY.get(A11Y_KEY)
        : localStorage.getItem(A11Y_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  function writeStore() {
    const raw = JSON.stringify(state);
    if (typeof PRIVACY !== 'undefined') { PRIVACY.set(A11Y_KEY, raw); return; }
    try { localStorage.setItem(A11Y_KEY, raw); } catch (e) { /* מצב פרטי */ }
  }

  function load() {
    const d = readStore();
    if (d) Object.assign(state, d);
    if (A11Y_MODES.indexOf(state.mode) < 0) state.mode = '';
    if (A11Y_FS.indexOf(state.fs) < 0) state.fs = 's';
  }

  /* ---------- החלה ----------
     גודל הטקסט הוא data-fs על <html>, וגיליון הסגנון מתרגם אותו לגודל
     בסיס. זה עובד רק אם מידות הטקסט באתר נמדדות ב-rem: px מתעלם
     משינוי font-size של השורש. זו התקלה הנפוצה ביותר בהטמעה הזו —
     scripts/audit.mjs בודק אותה במפורש. */

  function apply() {
    const r = document.documentElement;
    r.setAttribute('data-fs', state.fs);
    A11Y_MODES.forEach(m => r.classList.toggle('a11y-' + m, state.mode === m));
    A11Y_FLAGS.forEach(f => r.classList.toggle('a11y-' + f, !!state[f]));
    sync();
  }

  function sync() {
    document.querySelectorAll('[data-fs]').forEach(b => {
      if (b === document.documentElement) return;
      b.setAttribute('aria-pressed', String(b.dataset.fs === state.fs));
    });
    if (!panel) return;
    panel.querySelectorAll('[data-mode]').forEach(b => {
      b.setAttribute('aria-pressed', String((b.dataset.mode || '') === state.mode));
    });
    panel.querySelectorAll('[data-flag]').forEach(b => {
      b.setAttribute('aria-pressed', String(!!state[b.dataset.flag]));
    });
  }

  /* ---------- פעולות ---------- */

  function setFontSize(v) {
    if (A11Y_FS.indexOf(v) < 0) return;
    state.fs = v; writeStore(); apply(); announce('גודל הטקסט עודכן');
  }
  function setMode(m) {
    state.mode = (state.mode === m) ? '' : m;   /* לחיצה חוזרת מכבה */
    writeStore(); apply(); announce();
  }
  function toggleFlag(f) {
    state[f] = state[f] ? 0 : 1; writeStore(); apply(); announce();
  }
  function reset() {
    state = { fs: 's', mode: '', links: 0, readable: 0, spacing: 0, still: 0, cursor: 0, focus: 0 };
    writeStore(); apply(); announce('הגדרות הנגישות אופסו');
  }

  /* שינוי מצב אינו מזיז מיקוד, ולכן קורא מסך לא מדווח עליו מעצמו. */
  function announce(msg) {
    const live = document.getElementById('a11y-live');
    if (!live) return;
    live.textContent = msg || 'הגדרות הנגישות עודכנו';
    setTimeout(() => { live.textContent = ''; }, 1200);
  }

  /* ---------- נתיב יחסי לשורש האתר ----------
     כדי שקישורי המסמכים בתפריט יעבדו מכל עומק תיקייה. */
  function base() {
    const segs = location.pathname.split('/').filter(Boolean);
    const last = segs[segs.length - 1] || '';
    const depth = segs.length - (last.indexOf('.') > -1 ? 1 : 0);
    return depth > 0 ? '../'.repeat(depth) : '';
  }

  /* ---------- בנייה ---------- */

  function tog(flag, label) {
    return '<button type="button" class="a11y-tog" data-flag="' + flag + '" aria-pressed="false">' +
           '<span>' + label + '</span><span class="sw" aria-hidden="true"></span></button>';
  }

  function build() {
    const b = base();
    const cfg = window.A11Y_CONFIG || {};
    const privacyHref = cfg.privacyUrl || (b + 'privacy/');
    const a11yHref = cfg.accessibilityUrl || (b + 'accessibility/');

    fab = document.createElement('button');
    fab.type = 'button';
    fab.className = 'a11y-fab';
    fab.id = 'a11y-fab';
    fab.setAttribute('aria-expanded', 'false');
    fab.setAttribute('aria-controls', 'a11y-panel');
    fab.setAttribute('aria-label', 'תפריט נגישות (Alt+Shift+A)');
    fab.title = 'תפריט נגישות — Alt+Shift+A';
    fab.innerHTML =
      '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
      '<circle cx="12" cy="4" r="2"/>' +
      '<path d="M20 7.5c0 .8-.6 1.4-1.4 1.4L15 8.4v3.2l2.6 8.3a1.4 1.4 0 0 1-2.6 1L12.6 14h-1.2L9 20.9a1.4 1.4 0 0 1-2.6-1L9 11.6V8.4l-3.6.5A1.4 1.4 0 0 1 4 7.5c0-.8.6-1.4 1.4-1.4l6.6.9 6.6-.9c.8 0 1.4.6 1.4 1.4z"/>' +
      '</svg>';

    panel = document.createElement('div');
    panel.className = 'a11y-panel';
    panel.id = 'a11y-panel';
    panel.hidden = true;
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-modal', 'true');
    panel.setAttribute('aria-labelledby', 'a11y-title');
    panel.innerHTML =
      '<div class="a11y-hd">' +
        '<h2 id="a11y-title">נגישות</h2>' +
        '<button type="button" class="a11y-x" aria-label="סגירת תפריט הנגישות">✕</button>' +
      '</div>' +
      '<div class="a11y-grp"><h3>גודל טקסט</h3>' +
        '<div class="a11y-seg three" role="group" aria-label="גודל טקסט">' +
          '<button type="button" class="sz-s" data-fs="s" aria-pressed="false">קטן</button>' +
          '<button type="button" class="sz-m" data-fs="m" aria-pressed="false">בינוני</button>' +
          '<button type="button" class="sz-l" data-fs="l" aria-pressed="false">גדול</button>' +
        '</div></div>' +
      '<div class="a11y-grp"><h3>מצב תצוגה</h3>' +
        '<div class="a11y-seg" role="group" aria-label="מצב תצוגה">' +
          '<button type="button" data-mode="contrast" aria-pressed="false">ניגודיות גבוהה</button>' +
          '<button type="button" data-mode="invert" aria-pressed="false">ניגודיות הפוכה</button>' +
          '<button type="button" data-mode="mono" aria-pressed="false">גווני אפור</button>' +
          '<button type="button" data-mode="" aria-pressed="false">צבעי האתר</button>' +
        '</div></div>' +
      '<div class="a11y-grp"><h3>קריאוּת</h3>' +
        tog('readable', 'פונט קריא') +
        tog('spacing', 'ריווח שורות ואותיות') +
        tog('links', 'הדגשת קישורים') +
      '</div>' +
      '<div class="a11y-grp"><h3>ניווט</h3>' +
        tog('focus', 'הדגשת מיקוד מקלדת') +
        tog('cursor', 'סמן עכבר גדול') +
        tog('still', 'עצירת אנימציות') +
      '</div>' +
      '<div class="a11y-foot">' +
        '<button type="button" class="a11y-reset">איפוס הגדרות הנגישות</button>' +
        '<p class="a11y-links-row">' +
          '<a href="' + a11yHref + '">הצהרת נגישות</a> · ' +
          '<a href="' + privacyHref + '">מדיניות פרטיות</a>' +
        '</p>' +
        '<p class="a11y-kbd">Alt+Shift+A · Esc לסגירה</p>' +
      '</div>';

    const live = document.createElement('div');
    live.id = 'a11y-live';
    live.className = 'a11y-sr';
    live.setAttribute('role', 'status');
    live.setAttribute('aria-live', 'polite');

    /* מיקום ב-DOM: הכפתור והתפריט ממוקמים fixed, ולכן המיקום כאן אינו
       נראה — אבל הוא קובע מתי מגיעים אליהם ב-Tab. בסוף ה-body תפריט
       הנגישות הוא התחנה האחרונה בדף, אחרי כל התוכן; מיד אחרי קישור
       הדילוג הוא בהישג יד. */
    const skip = document.querySelector('a.skip, a[class*="skip"], a[href^="#"][class]');
    if (skip && skip.parentNode === document.body) skip.after(fab, panel, live);
    else document.body.prepend(fab, panel, live);

    fab.addEventListener('click', toggle);
    panel.querySelector('.a11y-x').addEventListener('click', close);
    panel.querySelector('.a11y-reset').addEventListener('click', reset);
    panel.querySelectorAll('[data-fs]').forEach(btn =>
      btn.addEventListener('click', () => setFontSize(btn.dataset.fs)));
    panel.querySelectorAll('[data-mode]').forEach(btn =>
      btn.addEventListener('click', () => setMode(btn.dataset.mode || '')));
    panel.querySelectorAll('[data-flag]').forEach(btn =>
      btn.addEventListener('click', () => toggleFlag(btn.dataset.flag)));

    /* לכידת מיקוד: כל עוד התפריט פתוח, Tab מסתובב בתוכו בלבד. */
    panel.addEventListener('keydown', e => {
      if (e.key !== 'Tab') return;
      const items = panel.querySelectorAll('button, a[href]');
      if (!items.length) return;
      const first = items[0], last = items[items.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });

    document.addEventListener('keydown', e => {
      if (e.key === 'Escape' && !panel.hidden) { close(); return; }
      /* code ולא key — כדי שהקיצור יעבוד גם בפריסת מקלדת עברית. */
      if (e.altKey && e.shiftKey && e.code === 'KeyA') { e.preventDefault(); toggle(); }
    });

    document.addEventListener('click', e => {
      if (panel.hidden) return;
      if (panel.contains(e.target) || fab.contains(e.target)) return;
      close();
    });

    measureDock();
  }

  /* גובה סרגל תחתון קבוע, אם יש כזה: נמדד ומתפרסם כמשתנה CSS, כדי
     שהכפתור יישב מעליו. מדידה ולא מספר קבוע, מפני שגובה סרגל כזה
     משתנה עם גודל הטקסט. */
  function measureDock() {
    const sel = (window.A11Y_CONFIG || {}).dockSelector || 'nav.tabbar, .bottom-bar, [data-a11y-dock]';
    const bar = document.querySelector(sel);
    if (!bar) return;
    const set = () => document.documentElement.style.setProperty('--a11y-dock', bar.offsetHeight + 'px');
    set();
    if (typeof ResizeObserver === 'function') new ResizeObserver(set).observe(bar);
    else window.addEventListener('resize', set);
  }

  /* ---------- פתיחה וסגירה ---------- */

  const CONTROL = 'a[href], button, input, select, textarea, summary, [tabindex]:not([tabindex="-1"])';

  function open() {
    panel.hidden = false;
    fab.setAttribute('aria-expanded', 'true');
    lastFocus = document.activeElement;
    const first = panel.querySelector('button, a[href]');
    if (first) first.focus();
  }
  function close() {
    if (panel.hidden) return;
    panel.hidden = true;
    fab.setAttribute('aria-expanded', 'false');
    /* חוזרים למקום שממנו נפתח התפריט — אבל רק אם זה פקד ממשי.
       פתיחה בקיצור מקלדת עלולה לתפוס את body או אלמנט שמוקד
       תכנותית, והחזרה לשם מאבדת את המשתמש. */
    if (lastFocus && document.contains(lastFocus) && lastFocus.matches && lastFocus.matches(CONTROL))
      lastFocus.focus();
    else
      fab.focus();
  }
  function toggle() { panel.hidden ? open() : close(); }

  function init() { load(); build(); apply(); }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  document.addEventListener('privacy:change', e => {
    if (e.detail && e.detail.local === false) reset();
  });

  return { open, close, toggle, reset, setFontSize, setMode, toggleFlag };
})();

/* גלובלי נוח, ותאימות לאתרים שכבר קוראים ל-setFontSize מ-onclick. */
if (typeof setFontSize === 'undefined') var setFontSize = A11Y.setFontSize;
