<<<<<<< HEAD
(made with help of manus.im)

# Python Backend & Automation
=======
# App Builder’s Workshop
>>>>>>> ace78a8 (Re-added option for concise course)

A dependency-free, static, multi-page learning course for beginning a Tauri + React + FastAPI + SQLite desktop app project.

## Run locally

From the project directory:

```bash
python3 -m http.server 3000 --bind 0.0.0.0
```

Open `http://127.0.0.1:3000/` locally. The Manus Preview is configured on port 3000. Use a static server rather than opening the HTML files directly so chapter links and browser storage behave consistently.

## Contents

- `index.html` — the course map and prerequisites.
- `chapters/` — expanded, linked lessons for the full learning path.
- `short-chapters/` — the earlier concise versions in their own folder, with a short-course index and links to expanded counterparts.
- `assets/site.css` — shared accessible styling, including light and dark themes.
- `assets/site.js` — persistent theme toggle plus local chapter progress, reading progress, and home-page filtering.
- `assets/course-mark.svg` — reusable workshop-course emblem: an open book with a small workshop spark, not tied to a single subject.
- `manus-routes.json` — route declaration required by the Webdev preview workflow.
- `plan.md` and `TODO.md` — approved design/implementation notes and project outcome tracker.

The course itself has no backend and stores the selected theme and optional completion checkmarks in local browser storage. The desktop architecture examples are educational; in particular, a packaged FastAPI sidecar needs deliberate lifecycle, local networking, and security design. See Chapter 10 before selecting an architecture.

From the main homepage, choose either the expanded lessons or the concise versions. The short-course checkmarks use a separate browser-storage key, so progress in one reading mode does not overwrite the other.


## Separate Python Backend & Automation course

The standalone Python course has its own landing page at `/python-course/`. It has 15 lessons organized into Python logic/data, robust scripting, API/scraping/browser integration, async/scheduling/queues, and production deployment/monitoring/security. Lessons build a reliable data-intake service incrementally. Use only APIs and websites you are authorized to access; prefer documented APIs, follow provider terms and rate limits, and do not attempt to bypass login or anti-bot controls.

The Python lessons’ editable Markdown sources live under `python-course/source/`; generated, standalone lesson pages live under `python-course/chapters/`. To regenerate them, create/activate a virtual environment, install `requirements-course-build.txt`, and run `python tools/render_python_course.py`. The generated HTML is static and does not require Markdown or Python when served.
