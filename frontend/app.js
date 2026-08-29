const dataFiles = {
  titles: "public/data/titles_dashboard.csv",
  genres: "public/data/top_10_genres_dashboard.csv",
  years: "public/data/content_added_per_year_dashboard.csv",
  types: "public/data/movies_vs_tv_ratio_dashboard.csv",
  countries: "public/data/top_10_countries_dashboard.csv",
  ratings: "public/data/rating_distribution_dashboard.csv",
};

const state = {
  titles: [],
  filteredTitles: [],
  summary: {
    genres: [],
    years: [],
    types: [],
    countries: [],
    ratings: [],
  },
};

function parseCsv(text) {
  const rows = [];
  let row = [];
  let value = "";
  let quoted = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && quoted && next === '"') {
      value += '"';
      index += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      row.push(value);
      value = "";
    } else if ((char === "\n" || char === "\r") && !quoted) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(value);
      if (row.some((cell) => cell !== "")) {
        rows.push(row);
      }
      row = [];
      value = "";
    } else {
      value += char;
    }
  }

  if (value || row.length) {
    row.push(value);
    rows.push(row);
  }

  const [headers, ...records] = rows;
  return records.map((record) =>
    headers.reduce((item, header, index) => {
      item[header] = record[index] || "";
      return item;
    }, {}),
  );
}

async function loadCsv(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Could not load ${path}`);
  }
  return parseCsv(await response.text());
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function uniqueValues(rows, key) {
  return [...new Set(rows.map((row) => row[key]).filter(Boolean))].sort();
}

function fillSelect(id, values) {
  const select = document.getElementById(id);
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
}

function numberValue(value) {
  return Number.parseInt(value, 10) || 0;
}

function countBy(rows, key) {
  return rows.reduce((totals, row) => {
    const value = row[key] || "Unknown";
    totals[value] = (totals[value] || 0) + 1;
    return totals;
  }, {});
}

function topValue(rows, key) {
  const counts = countBy(rows, key);
  const [best] = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  return best ? best[0] : "-";
}

function maxValue(rows, valueKey) {
  return Math.max(...rows.map((row) => numberValue(row[valueKey])), 1);
}

function renderColumnChart(containerId, rows, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  const max = maxValue(rows, valueKey);
  container.innerHTML = rows
    .map((row) => {
      const value = numberValue(row[valueKey]);
      const height = Math.max((value / max) * 100, 12);
      return `
        <div class="column" style="height: ${height}%" title="${escapeHtml(row[labelKey])}: ${value}">
          <span>${escapeHtml(row[labelKey])}</span>
        </div>
      `;
    })
    .join("");
}

function renderProgressList(containerId, rows, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  const max = maxValue(rows, valueKey);
  container.innerHTML = rows
    .map((row) => {
      const value = numberValue(row[valueKey]);
      const width = Math.max((value / max) * 100, 5);
      return `
        <div class="progress-row">
          <strong>${escapeHtml(row[labelKey])}</strong>
          <span>${value.toLocaleString()}</span>
          <div class="progress-track">
            <div class="progress-fill" style="width: ${width}%"></div>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderMiniColumns(containerId, rows, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  const max = maxValue(rows, valueKey);
  container.innerHTML = rows
    .map((row) => {
      const value = numberValue(row[valueKey]);
      const height = Math.max((value / max) * 100, 14);
      return `
        <div class="mini-column" style="height: ${height}%" title="${escapeHtml(row[labelKey])}: ${value}">
          <span>${escapeHtml(row[labelKey])}</span>
        </div>
      `;
    })
    .join("");
}

function renderDotList(containerId, rows, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  const max = maxValue(rows, valueKey);
  container.innerHTML = rows
    .slice(0, 6)
    .map((row) => {
      const value = numberValue(row[valueKey]);
      const activeDots = Math.max(Math.round((value / max) * 9), 1);
      const dots = Array.from({ length: 9 }, (_, index) =>
        `<span class="dot${index < activeDots ? " active" : ""}"></span>`,
      ).join("");
      return `
        <div class="dot-row">
          <strong>${escapeHtml(row[labelKey])}</strong>
          <span>${value}</span>
          <div class="dot-rack">${dots}</div>
        </div>
      `;
    })
    .join("");
}

function filterTitles() {
  const type = document.getElementById("typeFilter").value;
  const rating = document.getElementById("ratingFilter").value;
  const decade = document.getElementById("decadeFilter").value;
  const query = document.getElementById("searchInput").value.trim().toLowerCase();

  state.filteredTitles = state.titles.filter((title) => {
    const matchesType = type === "all" || title.type === type;
    const matchesRating = rating === "all" || title.rating === rating;
    const matchesDecade = decade === "all" || title.decade === decade;
    const matchesQuery =
      !query ||
      title.title.toLowerCase().includes(query) ||
      title.listed_in.toLowerCase().includes(query) ||
      title.country.toLowerCase().includes(query);

    return matchesType && matchesRating && matchesDecade && matchesQuery;
  });

  renderKpis();
  renderTable();
}

function renderKpis() {
  const rows = state.filteredTitles;
  const movieCount = rows.filter((row) => row.type === "Movie").length;
  const tvCount = rows.filter((row) => row.type === "TV Show").length;
  const loadedCount = state.titles.length || 1;
  const qualityScore = Math.round((rows.length / loadedCount) * 100);

  document.getElementById("totalTitles").textContent = rows.length.toLocaleString();
  document.getElementById("movieCount").textContent = movieCount.toLocaleString();
  document.getElementById("tvCount").textContent = tvCount.toLocaleString();
  document.getElementById("topCountry").textContent = topValue(rows, "country");
  document.getElementById("mixTotal").textContent = rows.length.toLocaleString();
  document.getElementById("qualityScore").textContent = `${qualityScore}%`;
  document.getElementById("pipelineInsight").textContent =
    `${rows.length} filtered records from ${state.titles.length} cleaned titles.`;
}

function renderTable() {
  const tbody = document.getElementById("titlesTable");
  document.getElementById("visibleCount").textContent = `${state.filteredTitles.length} rows`;
  tbody.innerHTML = state.filteredTitles
    .slice(0, 12)
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.title)}</td>
          <td>${escapeHtml(row.type)}</td>
          <td>${escapeHtml(row.country)}</td>
          <td>${escapeHtml(row.date_added_year)}</td>
          <td>${escapeHtml(row.rating)}</td>
          <td>${escapeHtml(row.listed_in)}</td>
        </tr>
      `,
    )
    .join("");
}

function attachFilters() {
  ["typeFilter", "ratingFilter", "decadeFilter", "searchInput"].forEach((id) => {
    document.getElementById(id).addEventListener("input", filterTitles);
  });
}

function setTheme(theme) {
  const isDark = theme === "dark";
  document.documentElement.dataset.theme = theme;
  document.getElementById("themeLabel").textContent = isDark ? "Dark" : "Light";
  document.getElementById("themeToggle").setAttribute("aria-pressed", String(isDark));
  localStorage.setItem("netflix-etl-theme", theme);
}

function attachThemeToggle() {
  const savedTheme = localStorage.getItem("netflix-etl-theme");
  const preferredTheme = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  setTheme(savedTheme || preferredTheme);

  document.getElementById("themeToggle").addEventListener("click", () => {
    const currentTheme = document.documentElement.dataset.theme || "light";
    setTheme(currentTheme === "dark" ? "light" : "dark");
  });
}

async function init() {
  attachThemeToggle();

  const [titles, genres, years, types, countries, ratings] = await Promise.all([
    loadCsv(dataFiles.titles),
    loadCsv(dataFiles.genres),
    loadCsv(dataFiles.years),
    loadCsv(dataFiles.types),
    loadCsv(dataFiles.countries),
    loadCsv(dataFiles.ratings),
  ]);

  state.titles = titles;
  state.filteredTitles = titles;
  state.summary = { genres, years, types, countries, ratings };

  fillSelect("typeFilter", uniqueValues(titles, "type"));
  fillSelect("ratingFilter", uniqueValues(titles, "rating"));
  fillSelect("decadeFilter", uniqueValues(titles, "decade"));

  renderColumnChart("yearChart", years, "year_added", "content_count");
  renderProgressList("typeChart", types, "type", "content_count");
  renderMiniColumns("ratingChart", ratings, "rating", "content_count");
  renderDotList("genreChart", genres, "genre", "content_count");
  renderDotList("countryChart", countries, "country", "content_count");

  attachFilters();
  filterTitles();
}

init().catch((error) => {
  document.body.innerHTML = `<main class="dashboard-shell"><h1>Dashboard data failed to load</h1><p>${escapeHtml(error.message)}</p></main>`;
});
