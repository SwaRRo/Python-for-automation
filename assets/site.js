(() => {
  const themeKey = "app-workshop-theme";
  const themeToggle = document.querySelector("[data-theme-toggle]");
  const syncThemeToggle = () => {
    if (!themeToggle) return;
    const isDark = document.documentElement.dataset.theme === "dark";
    themeToggle.setAttribute("aria-pressed", String(isDark));
    themeToggle.textContent = isDark ? "Dark mode: on" : "Dark mode: off";
  };
  syncThemeToggle();
  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const nextTheme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
      document.documentElement.dataset.theme = nextTheme;
      try { localStorage.setItem(themeKey, nextTheme); }
      catch { /* Theme still works for this page if storage is unavailable. */ }
      syncThemeToggle();
    });
  }

  const trackId = document.body.dataset.track || "frontend";
  const storageKey = trackId === "frontend"
    ? "erp-course-completed-v1"
    : `course-completed-${trackId}-v1`;
  const readCompleted = () => {
    try { return JSON.parse(localStorage.getItem(storageKey) || "[]"); }
    catch { return []; }
  };
  const writeCompleted = (ids) => {
    try { localStorage.setItem(storageKey, JSON.stringify(ids)); }
    catch { /* The lesson still works if storage is unavailable. */ }
  };
  const completed = new Set(readCompleted());
  const chapterId = document.body.dataset.chapter;
  const toggle = document.querySelector("[data-complete]");
  if (toggle && chapterId) {
    const syncToggle = () => {
      const isDone = completed.has(chapterId);
      toggle.setAttribute("aria-pressed", String(isDone));
      toggle.textContent = isDone ? "✓ Chapter complete — click to undo" : "Mark this chapter complete";
    };
    syncToggle();
    toggle.addEventListener("click", () => {
      if (completed.has(chapterId)) completed.delete(chapterId);
      else completed.add(chapterId);
      writeCompleted([...completed]);
      syncToggle();
      window.dispatchEvent(new CustomEvent("course-progress-change"));
    });
  }
  const progressBar = document.querySelector("[data-reading-progress]");
  if (progressBar) {
    const updateReading = () => {
      const max = document.documentElement.scrollHeight - window.innerHeight;
      const percent = max <= 0 ? 100 : Math.min(100, Math.max(0, window.scrollY / max * 100));
      progressBar.style.width = `${percent}%`;
    };
    window.addEventListener("scroll", updateReading, { passive: true });
    updateReading();
  }
  const progress = document.querySelector("[data-course-progress]");
  if (progress) {
    const total = Number(progress.dataset.total || 0);
    const updateCourse = () => {
      const count = readCompleted().length;
      const percent = total ? Math.min(100, count / total * 100) : 0;
      const fill = progress.querySelector(".progress-track span");
      const text = progress.querySelector("[data-progress-label]");
      if (fill) fill.style.width = `${percent}%`;
      if (text) text.textContent = `${Math.min(count, total)} of ${total} chapters complete`;
    };
    updateCourse();
    window.addEventListener("course-progress-change", updateCourse);
    window.addEventListener("storage", updateCourse);
  }
  const filter = document.querySelector("[data-chapter-filter]");
  if (filter) filter.addEventListener("input", () => {
    const query = filter.value.trim().toLowerCase();
    document.querySelectorAll("[data-chapter-card]").forEach((card) => {
      card.hidden = query !== "" && !card.textContent.toLowerCase().includes(query);
    });
  });
})();
