# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-wfp-adam** collects disaster event data from the [WFP ADAM API](https://api.adam.geospatial.wfp.org/api/collections) and publishes one HDX dataset per event. It handles three event types: earthquakes (SM), tropical storms (TS), and floods (FL). The scraper runs weekly and is stateful — it tracks the last run date via `HDXState` to fetch only new events since the previous run.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.wfp.adam
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_wfp.py::TestADAM::test_generate_datasets_and_showcases
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through three stages in `__main__.py`:

1. **`parse_feed`** — Calls the ADAM API for a given event type collection, filters out high-income countries (via `hdx-python-country`), deduplicates by `event_id`, and keeps only the latest episode per event. Returns `latest_episodes: dict`.

2. **`parse_eventtypes_feed`** — Enriches each episode with computed `name`, `title`, `description`, and `latitude`/`longitude` fields using `lazy_fstr` (Python `eval`-based f-string templates from YAML config). Returns a flat list of event dicts.

3. **`generate_dataset`** — Constructs an HDX `Dataset` object, downloads attached files (GeoJSON, shapefile, population CSV) via `retriever.download_file`, creates `Resource` objects for each, and optionally creates `Showcase` objects for dashboard URLs. Returns `(dataset, showcases)`.

### Key design points

- **Event type config is data-driven**: `project_configuration.yaml` defines each event type's `collection_id`, field templates (`name`, `title`, `description`), and HDX tag. Adding a new event type only requires a YAML entry.
- **`lazy_fstr`** in `pipeline.py` evaluates YAML string templates as f-strings at runtime using `eval()`, with the `event` dict in scope. Template strings reference event fields directly (e.g., `{event['mag']}`).
- **FL events use a different API query** than SM/TS: they filter by `cleared='yes'` and `effective` date rather than `published=true` and `published_at`.
- **Retriever** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` flags — used in tests to replay fixture data from `tests/fixtures/input/`.
- **`HDXState`** persists the last successful run date so the next run fetches only new events. The `state.set(now_utc())` call in `__main__.py` is currently commented out (in-progress work on the `new_api` branch).

### Config files

- `config/project_configuration.yaml` — API base URL and per-event-type config (collection ID, f-string templates for titles/descriptions/names, HDX tags)
- `config/hdx_dataset_static.yaml` — Static HDX metadata applied to every dataset (license, methodology, source, etc.)

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-wfp-adam` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
