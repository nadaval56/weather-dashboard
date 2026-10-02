/* ================= פרטיות =================
   שכבה דקה שעושה שלושה דברים:

   1. שומרת את בחירת המשתמש לגבי שמירה מקומית (localStorage) —
      ומאפשרת לשאר הקוד לשאול "מותר לשמור?" לפני כל כתיבה.
   2. מציגה את הודעת הפרטיות בכניסה הראשונה.
   3. מספקת את פקדי הנתונים שבדף מדיניות הפרטיות (עיון, ייצוא, מחיקה).

   (מקור: geniza-explorer/assets/privacy.js)

   הקובץ נטען ראשון בכל דף, לפני a11y.js ולפני קוד האחסון של האתר,
   כדי שהדגל יהיה זמין להם. אין כאן שום שליחה של מידע לשום מקום.

   התאמה לאתר: הגדר window.PRIVACY_CONFIG לפני טעינת הקובץ —
     { appPrefixes: ['myapp:'],      // קידומות המפתחות שהאתר שומר
       privacyUrl: '/privacy/',      // ברירת מחדל: נגזר מעומק הנתיב
       noticeHtml: '...' }           // נוסח חלופי להודעה
   ברירות המחדל סבירות לרוב האתרים.

   מפתח ההסכמה ('privacy:v1') הוא היחיד שנשמר גם כשהמשתמש בחר
   "בלי שמירה מקומית" — בלעדיו אי אפשר לכבד את הבחירה בביקור הבא.
   הדבר מפורט במפורש בדף מדיניות הפרטיות. */

const PRIVACY_KEY = 'privacy:v1';
const APP_PREFIXES = (window.PRIVACY_CONFIG || {}).appPrefixes || ['a11y:'];

var PRIVACY = (function () {

  function readRaw() {
    try {
      const raw = localStorage.getItem(PRIVACY_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  function writeRaw(o) {
    try { localStorage.setItem(PRIVACY_KEY, JSON.stringify(o)); } catch (e) { /* מצב פרטי */ }
  }

  /* האם המשתמש כבר ראה את ההודעה */
  function acknowledged() {
    const p = readRaw();
    return !!(p && p.ack);
  }

  /* ברירת המחדל היא כן: שמירה מקומית היא רק העדפות התצוגה,
     והיא נשארת על המכשיר. מי שבחר אחרת — מקבל אחרת. */
  function storageAllowed() {
    const p = readRaw();
    return !(p && p.local === false);
  }

  /* עטיפות בטוחות שכל שאר הקוד משתמש בהן במקום localStorage ישיר. */
  function get(k) {
    if (!storageAllowed()) return null;
    try { return localStorage.getItem(k); } catch (e) { return null; }
  }
  function set(k, v) {
    if (!storageAllowed()) return false;
    try { localStorage.setItem(k, v); return true; } catch (e) { return false; }
  }
  function remove(k) {
    try { localStorage.removeItem(k); } catch (e) { /* אין מה לעשות */ }
  }

  /* מוחק את כל מה שהאתר שמר, חוץ מרשומת ההסכמה עצמה. */
  function purge() {
    let keys = [];
    try { keys = Object.keys(localStorage); } catch (e) { return 0; }
    let n = 0;
    keys.forEach(function (k) {
      if (k === PRIVACY_KEY) return;
      if (APP_PREFIXES.some(function (p) { return k.indexOf(p) === 0; })) { remove(k); n++; }
    });
    return n;
  }

  /* רשימת מה שנשמר כרגע — משמשת את "זכות העיון" בדף הפרטיות. */
  function inventory() {
    let keys = [];
    try { keys = Object.keys(localStorage); } catch (e) { return []; }
    return keys
      .filter(function (k) {
        return k === PRIVACY_KEY || APP_PREFIXES.some(function (p) { return k.indexOf(p) === 0; });
      })
      .sort()
      .map(function (k) {
        let v = '';
        try { v = localStorage.getItem(k) || ''; } catch (e) { v = ''; }
        return { key: k, size: v.length, value: v };
      });
  }

  function decide(allowLocal) {
    if (!allowLocal) purge();
    writeRaw({ ack: true, local: !!allowLocal, ts: new Date().toISOString() });
    document.dispatchEvent(new CustomEvent('privacy:change', { detail: { local: !!allowLocal } }));
  }

  /* מאפס את ההסכמה — ההודעה תופיע שוב בטעינה הבאה. */
  function reopen() {
    remove(PRIVACY_KEY);
    document.dispatchEvent(new CustomEvent('privacy:change', { detail: { local: true } }));
  }

  return {
    acknowledged: acknowledged,
    storageAllowed: storageAllowed,
    get: get, set: set, remove: remove,
    purge: purge, inventory: inventory,
    decide: decide, reopen: reopen,
    KEY: PRIVACY_KEY
  };
})();

/* ================= הודעת הפרטיות =================
   פס בתחתית המסך, לא חלון חוסם: אפשר להמשיך לקרוא ולגלול בזמן
   שהוא פתוח, ושתי האפשרויות זהות בגודל ובנגישות. הוא לא חוזר
   אחרי בחירה, ואפשר לפתוח אותו שוב מדף מדיניות הפרטיות. */
(function () {
  if (PRIVACY.acknowledged()) return;

  function root() {
    /* עומק הנתיב קובע את הקידומת היחסית: '' בשורש, '../' בתת-תיקייה. */
    const segs = location.pathname.split('/').filter(Boolean);
    const last = segs[segs.length - 1] || '';
    const depth = segs.length - (last.indexOf('.') > -1 ? 1 : 0);
    return depth > 0 ? '../'.repeat(depth) : '';
  }

  function build() {
    const base = root();
    const bar = document.createElement('div');
    bar.className = 'consent';
    bar.setAttribute('role', 'region');
    bar.setAttribute('aria-label', 'הודעת פרטיות');
    const custom = (window.PRIVACY_CONFIG || {}).noticeHtml;
    bar.innerHTML = custom ? custom :
      '<div class="wrap">' +
        '<div class="consent-txt">' +
          '<b>הודעת פרטיות</b>' +
          '<p>האתר לא אוסף עליך מידע, לא משתמש בעוגיות, אין בו פרסומות ואין בו כלי מדידה. ' +
          'העדפות התצוגה שתבחר בתפריט הנגישות נשמרות <b>במכשיר שלך בלבד</b> (אחסון מקומי בדפדפן) ' +
          'ואינן נשלחות לשום שרת. הדף לא פונה לשום שירות חיצוני: כל הנתונים, כולל העננות, מגיעים מהאתר עצמו. ' +
          '<a href="' + ((window.PRIVACY_CONFIG || {}).privacyUrl || (base + 'privacy/')) + '">מדיניות הפרטיות המלאה</a></p>' +
        '</div>' +
        '<div class="consent-act">' +
          '<button type="button" class="consent-ok">אישור והמשך</button>' +
          '<button type="button" class="consent-no">בלי שמירה מקומית</button>' +
        '</div>' +
      '</div>';
    /* הפס ממוקם fixed, ולכן מיקומו ב-DOM אינו משפיע על העיצוב — אבל כן
       על סדר המקלדת. בסוף ה-body הוא היה תחנת Tab מספר 28 בערך, כלומר
       הודעה שמבקשת החלטה שאי אפשר להגיע אליה. אחרי קישור הדילוג היא
       בהישג יד מיד. */
    const skip = document.querySelector('a.skip, a[class*="skip"]');
    if (skip && skip.parentNode === document.body) skip.after(bar);
    else document.body.insertBefore(bar, document.body.firstChild);

    /* גובה ההודעה נמדד ונשמר כמשתנה CSS, כדי שכפתור הנגישות
       יעלה מעליה ולא יוסתר. */
    function measure() {
      document.documentElement.style.setProperty('--a11y-consent-h', bar.offsetHeight + 'px');
    }
    measure();
    window.addEventListener('resize', measure);

    function close(allowLocal) {
      PRIVACY.decide(allowLocal);
      bar.remove();
      window.removeEventListener('resize', measure);
      document.documentElement.style.setProperty('--a11y-consent-h', '0px');
      /* ההודעה נמחקה מה-DOM ואיתה הכפתור שהיה במיקוד. בלי ההעברה הזו
         המיקוד נופל ל-body, ומי שמנווט במקלדת מאבד את מקומו בדף. */
      const main = document.querySelector('main');
      if (main) {
        if (!main.hasAttribute('tabindex')) main.setAttribute('tabindex', '-1');
        main.focus({ preventScroll: true });
      }
    }
    bar.querySelector('.consent-ok').addEventListener('click', function () { close(true); });
    bar.querySelector('.consent-no').addEventListener('click', function () {
      /* בכניסה ראשונה אין עדיין מה למחוק, אבל אפשר לפתוח את ההודעה
         שוב מדף הפרטיות — ואז הביטול מוחק העדפות קיימות. לא מוחקים
         נתונים של מישהו בלי שהוא יידע. */
      const has = PRIVACY.inventory().some(function (r) { return r.key !== PRIVACY.KEY; });
      if (has && !confirm('ביטול השמירה המקומית ימחק עכשיו את העדפות התצוגה ' +
                          'שנשמרו במכשיר הזה. להמשיך?')) return;
      close(false);
    });
  }

  if (document.readyState === 'loading')
    document.addEventListener('DOMContentLoaded', build);
  else
    build();
})();
