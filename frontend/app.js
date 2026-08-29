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

function renderBars(containerId, rows, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  const max = Math.max(...rows.map((row) => numberValue(row[valueKey])), 1);
  container.innerHTML = rows
    .map((row) => {
      const value = numberValue(row[valueKey]);
      const width = Math.max((value / max) * 100, 4);
      return `
        <div class="bar-row">
          <span class="bar-label" title="${row[labelKey]}">${row[labelKey]}</span>
          <span class="bar-track"><span class="bar-fill" style="width: ${width}%"></span></span>
          <span class="bar-value">${value}</span>
        </div>
      `;
    })
    .join("");
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
  document.getElementById("totalTitles").textContent = rows.length;
  document.getElementById("movieCount").textContent = rows.filter((row) => row.type === "Movie").length;
  document.getElementById("tvCount").textContent = rows.filter((row) => row.type === "TV Show").length;
  document.getElementById("topCountry").textContent = topValue(rows, "country");
}

function renderTable() {
  const tbody = document.getElementById("titlesTable");
  document.getElementById("visibleCount").textContent = `${state.filteredTitles.length} rows`;
  tbody.innerHTML = state.filteredTitles
    .slice(0, 12)
    .map(
      (row) => `
        <tr>
          <td>${row.title}</td>
          <td>${row.type}</td>
          <td>${row.country}</td>
          <td>${row.date_added_year}</td>
          <td>${row.rating}</td>
          <td>${row.listed_in}</td>
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

async function init() {
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

  fillSelect("typeFilter", uniqueValues(titles, "type"));
  fillSelect("ratingFilter", uniqueValues(titles, "rating"));
  fillSelect("decadeFilter", uniqueValues(titles, "decade"));

  renderBars("yearChart", years, "year_added", "content_count");
  renderBars("typeChart", types, "type", "content_count");
  renderBars("genreChart", genres, "genre", "content_count");
  renderBars("countryChart", countries, "country", "content_count");
  renderBars("ratingChart", ratings, "rating", "content_count");

  attachFilters();
  filterTitles();
}

init().catch((error) => {
  document.body.innerHTML = `<main class="panel"><h1>Dashboard data failed to load</h1><p>${error.message}</p></main>`;
});
