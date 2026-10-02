/* ================= פס האתרים הנוספים =================
   (מקור: geniza-explorer/assets/sister.js)
   הפס הוא אזור גלילה רגיל: אפשר להחליק אותו באצבע או לגלול בעכבר.
   הקוד כאן רק מזיז אותו לאט כשאף אחד לא נוגע בו, ועוצר:
     - כל עוד אצבע, עכבר או מיקוד מקלדת נמצאים עליו (WCAG 2.2.2);
     - לעוד כמה שניות אחרי שהמשתמש הפסיק, כדי שהתנועה לא "תחטוף" את
       הגלילה שלו (כולל תנופת הגלילה שנמשכת אחרי הרמת האצבע);
     - כשהפס לא נראה במסך.
   ברשימה שני עותקים (השני מוסתר מקורא המסך ומהמקלדת). כשהגלילה מגיעה
   לקצה של עותק אחד היא קופצת ברוחב עותק שלם, לאותה נקודה בדיוק במראה,
   ולכן הלולאה רציפה לשני הכיוונים.
   במצב "בלי תנועה" (הגדרת המערכת, או "עצירת אנימציות" בתפריט הנגישות) אין תנועה אוטומטית
   וההעתק מוסתר ב-CSS; נשאר פס רגיל שגוללים ביד. */
(function () {
  var view = document.querySelector('.sister-view');
  if (!view) return;
  var sets = view.querySelectorAll('.sister-set');
  if (sets.length < 2) return;

  var SPEED = 18;          /* פיקסלים לשנייה */
  var RESUME_MS = 2500;    /* המתנה אחרי מגע לפני שהתנועה חוזרת */
  var reduce = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;
  var holds = 0, resumeAt = 0, visible = true, pos = null, last = 0;

  function still() {
    return (reduce && reduce.matches) || document.documentElement.classList.contains('a11y-still');
  }
  /* ב-RTL scrollLeft הוא 0 בקצה הימני ושלילי לכיוון שמאל (בכל הדפדפנים
     המודרניים). התנועה היא ימינה, כלומר חשיפת פריטים משמאל: scrollLeft יורד. */
  function setW() { return Math.abs(sets[0].offsetLeft - sets[1].offsetLeft); }
  function wrap(x) {
    var w = setW(), max = view.scrollWidth - view.clientWidth;
    if (!w || w >= max) return x;
    if (x > -1) x -= w;                 /* קרוב לקצה הימני: אותה נקודה בעותק השני */
    if (x < -(max - 1)) x += w;         /* קרוב לקצה השמאלי: חזרה לעותק הראשון */
    return x;
  }
  function hold() { holds++; }
  function release() { holds = Math.max(0, holds - 1); resumeAt = performance.now() + RESUME_MS; pos = null; }

  function tick(t) {
    var dt = last ? Math.min(t - last, 100) : 0;
    last = t;
    if (!still() && visible && !holds && t >= resumeAt) {
      if (pos === null) pos = wrap(view.scrollLeft);
      pos = wrap(pos - SPEED * dt / 1000);
      view.scrollLeft = pos;
    } else if (!holds && t < resumeAt) {
      /* בזמן ההמתנה: שומרים על הלולאה גם כשהמשתמש גלל עד הקצה ביד */
      var w = wrap(view.scrollLeft);
      if (w !== view.scrollLeft) view.scrollLeft = w;
    }
    requestAnimationFrame(tick);
  }

  /* מגע ועכבר: עוצרים בזמן הנגיעה, וממשיכים כמה שניות אחרי */
  view.addEventListener('touchstart', hold, { passive: true });
  view.addEventListener('touchend', release, { passive: true });
  view.addEventListener('touchcancel', release, { passive: true });
  view.addEventListener('mouseenter', hold);
  view.addEventListener('mouseleave', release);
  view.addEventListener('focusin', hold);
  view.addEventListener('focusout', release);
  view.addEventListener('wheel', function () { resumeAt = performance.now() + RESUME_MS; pos = null; }, { passive: true });

  if ('IntersectionObserver' in window) {
    new IntersectionObserver(function (e) { visible = e[0].isIntersecting; }).observe(view);
  }
  requestAnimationFrame(tick);
})();
