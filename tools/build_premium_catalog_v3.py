#!/usr/bin/env python3
"""Build PremiumCatalog v3 shards/final DB from Apple Music Feed song exports.

v3 keeps one row per song in tracks and moves storefront availability/score to
track_storefronts. It intentionally does not cap tracks per duration or per
storefront.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import tempfile
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


API_ROOT = "https://api.media.apple.com"
DEFAULT_STOREFRONTS = [
    "tw", "jp", "us", "kr", "hk", "sg", "gb", "ca", "au", "nz",
    "de", "fr", "it", "es", "br", "mx", "th", "id", "ph", "my",
    "vn", "in", "se", "nl", "ch", "at", "be", "ie", "dk", "no",
    "fi", "pl", "tr", "za", "ae", "sa", "cl", "co", "ar", "pe",
]
LOCALES = {
    "tw": "zh-Hant",
    "jp": "ja",
    "kr": "ko",
    "cn": "zh-Hans",
    "hk": "zh-Hant",
    "br": "pt-BR",
    "mx": "es",
    "es": "es",
    "fr": "fr",
    "de": "de",
    "it": "it",
}
TARGET_COLUMNS = [
    "id",
    "name",
    "nameDefault",
    "primaryArtists",
    "album",
    "isrc",
    "releaseDate",
    "durationInMillis",
    "parentalAdvisoryType",
    "genres",
    "prices",
    "audioLocale",
    "relevanceScore",
]
BLOCKED_TERMS = [
    "karaoke", "tribute", "sound effect", "sound effects", "ringtone",
    "alarm tone", "test tone", "white noise", "brown noise", "pink noise",
    "sped up", "slowed", "nightcore", "8 bit", "lofi cover",
    "made famous by", "as made famous", "originally performed by",
    "instrumental version", "backing track", "music box", "sleep sound",
    "sleep sounds", "sleep music", "sleep aid", "rain sounds",
    "rain for sleep", "sons de chuva", "para dormir", "sonidos de lluvia",
    "bruits de pluie", "meditacao", "meditação", "relaxing sounds",
    "cover version", "commentary", "chapter", "episode", "kapitel",
    "chapitre", "capitulo", "capítulo", "episodio", "hörspiel",
    "hoerspiel", "kurzhörspiel", "meditation", "hypnosis", "affirmation",
    "affirmations", "nursery rhyme",
]
BLOCKED_GENRES = {
    "audiobooks",
    "spoken word",
    "comedy",
    "children's music",
    "nature",
}


TRACKS_SCHEMA = """
CREATE TABLE IF NOT EXISTS tracks(
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    title_names TEXT NOT NULL DEFAULT '{}',
    artist_name TEXT NOT NULL,
    primary_artist_id TEXT NOT NULL DEFAULT '',
    artist_ids TEXT NOT NULL DEFAULT '[]',
    album_title TEXT NOT NULL,
    album_id TEXT,
    duration_seconds INTEGER NOT NULL,
    artwork_url TEXT,
    genre_names TEXT NOT NULL,
    isrc TEXT,
    audio_locale TEXT,
    release_date TEXT,
    content_rating TEXT,
    quality_tags TEXT NOT NULL,
    source_version INTEGER NOT NULL,
    global_relevance_score INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS track_storefronts(
    track_id TEXT NOT NULL,
    storefront TEXT NOT NULL,
    relevance_score INTEGER NOT NULL,
    global_relevance_score INTEGER NOT NULL,
    chart_rank INTEGER NOT NULL DEFAULT 9999,
    locale TEXT,
    source TEXT NOT NULL,
    last_validated_at REAL,
    PRIMARY KEY(track_id, storefront)
);

CREATE TABLE IF NOT EXISTS metadata(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


FINAL_INDEXES = """
CREATE INDEX IF NOT EXISTS tracks_duration_global_relevance_idx
ON tracks(duration_seconds, global_relevance_score DESC);
CREATE INDEX IF NOT EXISTS tracks_primary_artist_duration_idx
ON tracks(primary_artist_id, duration_seconds, global_relevance_score DESC);
CREATE INDEX IF NOT EXISTS track_storefronts_storefront_score_idx
ON track_storefronts(storefront, relevance_score DESC, track_id);
CREATE INDEX IF NOT EXISTS track_storefronts_track_idx
ON track_storefronts(track_id);
"""


def request_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def latest_export_id(feed_name: str, token: str) -> str:
    payload = request_json(f"{API_ROOT}/v1/feed/{feed_name}/latest", token)
    return payload["data"][0]["id"]


def export_parts(export_id: str, token: str, page_size: int = 100) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    next_path: str | None = f"/v1/feed/exports/{export_id}/parts?limit={page_size}"
    while next_path:
        payload = request_json(urllib.parse.urljoin(API_ROOT, next_path), token)
        resources = payload.get("resources", {}).get("parts", {})
        for item in payload.get("data", []):
            resource = resources[item["id"]]
            attributes = resource["attributes"]
            parts.append(
                {
                    "id": item["id"],
                    "offset": int(attributes.get("offset", len(parts))),
                    "url": attributes["exportLocation"],
                }
            )
        next_path = payload.get("next")
    return parts


def token_from_args(args: argparse.Namespace) -> str:
    token = os.environ.get("APPLE_MUSIC_DEVELOPER_TOKEN")
    if token:
        return token.strip()
    if args.token_file and Path(args.token_file).exists():
        return Path(args.token_file).read_text(encoding="utf-8").strip()
    raise SystemExit("Set APPLE_MUSIC_DEVELOPER_TOKEN or pass --token-file")


def bundled_ids(seed_db: Path | None) -> set[str]:
    if not seed_db or not seed_db.exists():
        return set()
    with sqlite3.connect(seed_db) as connection:
        return {str(row[0]) for row in connection.execute("SELECT DISTINCT id FROM tracks")}


def first_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        for item in value:
            if isinstance(item, tuple) and len(item) == 2 and item[0] == "default":
                return str(item[1]).strip() or None
            if isinstance(item, dict) and item.get("locale") == "default":
                return str(item.get("value", "")).strip() or None
    return None


def text_map(value: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    if isinstance(value, dict):
        for key, item in value.items():
            locale = str(key or "").strip()
            text = str(item or "").strip()
            if locale and text:
                result[locale] = text
        return result
    if isinstance(value, list):
        for item in value:
            if isinstance(item, tuple) and len(item) == 2:
                locale = str(item[0] or "").strip()
                text = str(item[1] or "").strip()
            elif isinstance(item, dict):
                locale = str(item.get("key") or item.get("locale") or "").strip()
                text = str(item.get("value") or "").strip()
            else:
                continue
            if locale and text:
                result[locale] = text
    return result


def artist_name(primary_artists: Any) -> str | None:
    if not isinstance(primary_artists, list):
        return None
    names = [
        str(artist.get("name", "")).strip()
        for artist in primary_artists
        if isinstance(artist, dict) and str(artist.get("name", "")).strip()
    ]
    return ", ".join(names[:3]) if names else None


def artist_ids(primary_artists: Any) -> list[str]:
    if not isinstance(primary_artists, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for artist in primary_artists:
        if not isinstance(artist, dict):
            continue
        artist_id = str(artist.get("id") or "").strip()
        if artist_id and artist_id not in seen:
            result.append(artist_id)
            seen.add(artist_id)
    return result[:3]


def album_title(album: Any) -> str | None:
    if not isinstance(album, dict):
        return None
    title = str(album.get("name", "")).strip()
    return title or None


def album_id(album: Any) -> str | None:
    if not isinstance(album, dict):
        return None
    value = str(album.get("id") or "").strip()
    return value or None


def genre_names(genres: Any) -> list[str]:
    if not isinstance(genres, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for genre in genres:
        if not isinstance(genre, dict):
            continue
        name = str(genre.get("name", "")).strip()
        key = name.lower()
        if name and key and key != "music" and key not in seen:
            result.append(name)
            seen.add(key)
    return result[:6]


def relevance_scores(relevance: Any) -> dict[str, int]:
    if not isinstance(relevance, dict):
        return {}
    scores: dict[str, int] = {}
    default_score = int(relevance.get("defaultScore") or 0)
    if default_score > 0:
        scores["default"] = default_score
    for item in relevance.get("storefrontOverrides") or []:
        if isinstance(item, tuple) and len(item) == 2:
            key = str(item[0]).lower()
            value = int(item[1] or 0)
        elif isinstance(item, dict):
            key = str(item.get("key", "")).lower()
            value = int(item.get("value") or 0)
        else:
            continue
        if key:
            scores[key] = max(scores.get(key, 0), value)
    return scores


def effective_relevance(scores: dict[str, int], storefront: str) -> int:
    return max(scores.get("default", 0), scores.get(storefront, 0))


def release_date_value(release_date: Any) -> str | None:
    if isinstance(release_date, str):
        return release_date.strip() or None
    values = text_map(release_date)
    return values.get("default") or first_text(values)


def streaming_storefronts(prices: Any, storefronts: set[str]) -> set[str]:
    available: set[str] = set()
    if not isinstance(prices, list):
        return available
    for item in prices:
        if not isinstance(item, tuple) or len(item) != 2:
            continue
        storefront = str(item[0]).lower()
        if storefront not in storefronts or storefront in available:
            continue
        price_entries = item[1] or []
        if any(isinstance(price, dict) and price.get("priceType") == "streaming" for price in price_entries):
            available.add(storefront)
            if len(available) == len(storefronts):
                break
    return available


def quality_tags(title: str, album: str | None, explicit: bool) -> list[str]:
    text = f"{title} {album or ''}".lower()
    tags: list[str] = []
    if "live" in text or "acoustic session" in text:
        tags.append("live")
    if any(term in text for term in ["remix", "remaster", "demo", "version"]):
        tags.append("alternate")
    if explicit:
        tags.append("explicit")
    return tags


def passes_quality_gate(title: str, artist: str, album: str | None, genres: list[str]) -> bool:
    if not title or not artist or not album or not genres:
        return False
    joined = " ".join([title, artist, album, " ".join(genres)]).lower()
    normalized = re.sub(r"[^a-z0-9ぁ-ゟ゠-ヿ一-龯々ー가-힣]+", " ", joined)
    if any(term in joined or term in normalized for term in BLOCKED_TERMS):
        return False
    if any(genre.lower() in BLOCKED_GENRES for genre in genres):
        return False
    if "various artists" in artist.lower():
        return False
    return True


def download_part(part: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"part-{int(part['offset']):05d}.gz.parquet"
    if path.exists() and path.stat().st_size > 0:
        return path
    temporary = path.with_suffix(path.suffix + ".tmp")
    with urllib.request.urlopen(part["url"], timeout=300) as response, temporary.open("wb") as file:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            file.write(chunk)
    temporary.replace(path)
    return path


def configure_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -200000;
        """
    )
    connection.executescript(TRACKS_SCHEMA)
    return connection


def insert_batches(
    connection: sqlite3.Connection,
    tracks: list[tuple[Any, ...]],
    storefronts: list[tuple[Any, ...]],
) -> None:
    if tracks:
        connection.executemany(
            """
            INSERT OR IGNORE INTO tracks(
                id, title, title_names, artist_name, primary_artist_id, artist_ids,
                album_title, album_id, duration_seconds, artwork_url, genre_names,
                isrc, audio_locale, release_date, content_rating, quality_tags,
                source_version, global_relevance_score
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            tracks,
        )
    if storefronts:
        connection.executemany(
            """
            INSERT OR REPLACE INTO track_storefronts(
                track_id, storefront, relevance_score, global_relevance_score,
                chart_rank, locale, source, last_validated_at
            )
            VALUES(?,?,?,?,?,?,?,?)
            """,
            storefronts,
        )


def process_parquet_part(
    path: Path,
    connection: sqlite3.Connection,
    seed_ids: set[str],
    storefronts: list[str],
    source_version: int,
    generated_at: str,
    drop_if_max_relevance_at_most: int,
    minimum_storefront_relevance: int,
    progress_prefix: str = "",
    progress_every_batches: int = 25,
) -> Counter[str]:
    stats: Counter[str] = Counter()
    storefront_set = set(storefronts)
    generated_timestamp = dt.datetime.fromisoformat(generated_at.replace("Z", "+00:00")).timestamp()
    parquet = pq.ParquetFile(path)
    columns = [column for column in TARGET_COLUMNS if column in parquet.schema_arrow.names]
    track_batch: list[tuple[Any, ...]] = []
    storefront_batch: list[tuple[Any, ...]] = []

    for batch_index, batch in enumerate(parquet.iter_batches(batch_size=8_000, columns=columns), start=1):
        for row in batch.to_pylist():
            stats["raw"] += 1
            song_id = str(row.get("id") or "").strip()
            if not song_id or song_id in seed_ids:
                stats["dropped_seed_or_missing_id"] += 1
                continue

            scores = relevance_scores(row.get("relevanceScore"))
            max_score = max(scores.values(), default=0)
            if max_score <= drop_if_max_relevance_at_most:
                stats["dropped_low_global_relevance"] += 1
                continue

            duration_ms = row.get("durationInMillis")
            if not duration_ms:
                stats["dropped_missing_duration"] += 1
                continue
            seconds = round(int(duration_ms) / 1000)
            if seconds < 1 or seconds > 600:
                stats["dropped_duration"] += 1
                continue

            available = streaming_storefronts(row.get("prices"), storefront_set)
            if not available:
                stats["dropped_not_streaming_in_target_storefronts"] += 1
                continue

            title_names = text_map(row.get("name"))
            title = first_text(row.get("nameDefault")) or title_names.get("default") or first_text(title_names)
            primary_artists = row.get("primaryArtists")
            artist = artist_name(primary_artists)
            primary_artist_ids = artist_ids(primary_artists)
            album = album_title(row.get("album"))
            genres = genre_names(row.get("genres"))
            if not title or not artist or not passes_quality_gate(title, artist, album, genres):
                stats["dropped_metadata_or_quality"] += 1
                continue

            storefront_rows: list[tuple[Any, ...]] = []
            for storefront in available:
                local_score = effective_relevance(scores, storefront)
                if local_score < minimum_storefront_relevance:
                    stats["dropped_low_storefront_relevance"] += 1
                    continue
                storefront_rows.append(
                    (
                        song_id,
                        storefront,
                        local_score,
                        max_score,
                        9999,
                        LOCALES.get(storefront, "en"),
                        "premium-feed-v3",
                        generated_timestamp,
                    )
                )

            if not storefront_rows:
                stats["dropped_no_storefront_after_relevance"] += 1
                continue

            explicit = row.get("parentalAdvisoryType") == "explicit"
            track_batch.append(
                (
                    song_id,
                    title,
                    json.dumps(title_names, ensure_ascii=False, separators=(",", ":")),
                    artist,
                    primary_artist_ids[0] if primary_artist_ids else "",
                    json.dumps(primary_artist_ids, ensure_ascii=False, separators=(",", ":")),
                    album,
                    album_id(row.get("album")),
                    seconds,
                    None,
                    json.dumps(genres, ensure_ascii=False, separators=(",", ":")),
                    str(row.get("isrc") or "").strip() or None,
                    str(row.get("audioLocale") or "").strip() or None,
                    release_date_value(row.get("releaseDate")),
                    "explicit" if explicit else None,
                    json.dumps(quality_tags(title, album, explicit), ensure_ascii=False, separators=(",", ":")),
                    source_version,
                    max_score,
                )
            )
            storefront_batch.extend(storefront_rows)
            stats["retained_tracks"] += 1
            stats["retained_storefront_rows"] += len(storefront_rows)

            if len(track_batch) >= 5_000:
                insert_batches(connection, track_batch, storefront_batch)
                track_batch.clear()
                storefront_batch.clear()
        if progress_prefix and batch_index % progress_every_batches == 0:
            print(
                f"{progress_prefix}: batches={batch_index} raw={stats['raw']} "
                f"retainedTracks={stats['retained_tracks']} storefrontRows={stats['retained_storefront_rows']}",
                flush=True,
            )

    insert_batches(connection, track_batch, storefront_batch)
    return stats


def shard_command(args: argparse.Namespace) -> None:
    token = token_from_args(args)
    export_id = args.song_export_id
    if export_id == "latest":
        export_id = latest_export_id("song", token)
    parts = export_parts(export_id, token)
    selected = [
        part for part in parts
        if args.offset_start <= int(part["offset"]) <= args.offset_end
    ]
    if not selected:
        raise SystemExit("No matching parts for requested offset range")

    output = Path(args.output)
    if output.exists() and not args.resume:
        output.unlink()
    connection = configure_connection(output)
    seed_ids = bundled_ids(Path(args.seed_db) if args.seed_db else None)
    storefronts = [item.lower() for item in (args.storefront or DEFAULT_STOREFRONTS)]
    generated_at = args.generated_at or dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    source_version = int(args.source_version or dt.datetime.now(dt.timezone.utc).timestamp())
    stats: Counter[str] = Counter()
    temp_root = Path(args.temp_dir) if args.temp_dir else None
    started = time.time()
    try:
        for index, part in enumerate(selected, start=1):
            with tempfile.TemporaryDirectory(prefix="premium-v3-part-", dir=temp_root) as temporary_dir:
                part_started = time.time()
                print(
                    f"shard {args.offset_start}-{args.offset_end}: start {index}/{len(selected)} "
                    f"offset={part['offset']}",
                    flush=True,
                )
                path = download_part(part, Path(temporary_dir))
                print(
                    f"shard {args.offset_start}-{args.offset_end}: downloaded offset={part['offset']} "
                    f"bytes={path.stat().st_size}",
                    flush=True,
                )
                connection.execute("BEGIN")
                part_stats = process_parquet_part(
                    path=path,
                    connection=connection,
                    seed_ids=seed_ids,
                    storefronts=storefronts,
                    source_version=source_version,
                    generated_at=generated_at,
                    drop_if_max_relevance_at_most=args.drop_if_max_relevance_at_most,
                    minimum_storefront_relevance=args.minimum_storefront_relevance,
                    progress_prefix=f"shard {args.offset_start}-{args.offset_end} offset={part['offset']}",
                )
                connection.execute("COMMIT")
                stats.update(part_stats)
            print(
                f"shard {args.offset_start}-{args.offset_end}: done {index}/{len(selected)} "
                f"offset={part['offset']} partRetainedTracks={part_stats['retained_tracks']} "
                f"totalRetainedTracks={stats['retained_tracks']} "
                f"storefrontRows={stats['retained_storefront_rows']} "
                f"partElapsed={time.time() - part_started:.1f}s elapsed={time.time() - started:.1f}s",
                flush=True,
            )
        connection.executescript(FINAL_INDEXES)
        metadata = {
            "premiumSchemaVersion": "3",
            "premiumSource": "apple-music-feed-song",
            "premiumSourceExportId": export_id,
            "premiumGeneratedAt": generated_at,
            "premiumSourceVersion": str(source_version),
            "offsetStart": str(args.offset_start),
            "offsetEnd": str(args.offset_end),
            "dropIfMaxRelevanceAtMost": str(args.drop_if_max_relevance_at_most),
            "minimumStorefrontRelevance": str(args.minimum_storefront_relevance),
            "seedIDCount": str(len(seed_ids)),
        }
        metadata.update({f"stats.{key}": str(value) for key, value in stats.items()})
        connection.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)",
            sorted(metadata.items()),
        )
        connection.commit()
        connection.execute("ANALYZE")
        connection.execute("VACUUM")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise
    finally:
        connection.close()
    print(f"Wrote shard {output} bytes={output.stat().st_size}", flush=True)


def merge_command(args: argparse.Namespace) -> None:
    output = Path(args.output)
    if output.exists():
        output.unlink()
    connection = configure_connection(output)
    shard_paths = sorted(Path(args.shard_dir).glob(args.shard_glob))
    if not shard_paths:
        raise SystemExit(f"No shards found: {args.shard_dir}/{args.shard_glob}")
    stats: Counter[str] = Counter()
    started = time.time()
    try:
        connection.execute("BEGIN")
        for index, shard in enumerate(shard_paths, start=1):
            shard_connection = sqlite3.connect(shard)
            try:
                track_rows = shard_connection.execute(
                    """
                    SELECT id, title, title_names, artist_name, primary_artist_id, artist_ids,
                           album_title, album_id, duration_seconds, artwork_url, genre_names,
                           isrc, audio_locale, release_date, content_rating, quality_tags,
                           source_version, global_relevance_score
                    FROM tracks
                    """
                ).fetchall()
                storefront_rows = shard_connection.execute(
                    """
                    SELECT track_id, storefront, relevance_score, global_relevance_score,
                           chart_rank, locale, source, last_validated_at
                    FROM track_storefronts
                    """
                ).fetchall()
            finally:
                shard_connection.close()
            insert_batches(connection, track_rows, storefront_rows)
            stats["input_shards"] += 1
            stats["input_track_rows"] += len(track_rows)
            stats["input_storefront_rows"] += len(storefront_rows)
            if index == 1 or index % 10 == 0 or index == len(shard_paths):
                print(
                    f"merged {index}/{len(shard_paths)} shards "
                    f"inputTracks={stats['input_track_rows']} inputStorefronts={stats['input_storefront_rows']} "
                    f"elapsed={time.time() - started:.1f}s",
                    flush=True,
                )
        connection.execute("COMMIT")
        connection.executescript(FINAL_INDEXES)
        counts = {
            "trackCount": connection.execute("SELECT COUNT(*) FROM tracks").fetchone()[0],
            "storefrontRowCount": connection.execute("SELECT COUNT(*) FROM track_storefronts").fetchone()[0],
            "distinctArtists": connection.execute("SELECT COUNT(DISTINCT primary_artist_id) FROM tracks WHERE primary_artist_id <> ''").fetchone()[0],
            "minimumDurationSeconds": connection.execute("SELECT MIN(duration_seconds) FROM tracks").fetchone()[0],
            "maximumDurationSeconds": connection.execute("SELECT MAX(duration_seconds) FROM tracks").fetchone()[0],
            "distinctDurationSeconds": connection.execute("SELECT COUNT(DISTINCT duration_seconds) FROM tracks").fetchone()[0],
        }
        metadata = {
            "premiumSchemaVersion": "3",
            "premiumSource": "apple-music-feed-song",
            "premiumTrackCount": str(counts["trackCount"]),
            "premiumStorefrontRowCount": str(counts["storefrontRowCount"]),
        }
        metadata.update({f"stats.{key}": str(value) for key, value in stats.items()})
        connection.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)",
            sorted(metadata.items()),
        )
        connection.commit()
        connection.execute("ANALYZE")
        connection.execute("VACUUM")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    report = {"output": str(output), "bytes": output.stat().st_size, "stats": dict(stats)}
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    shard = subparsers.add_parser("shard")
    shard.add_argument("--song-export-id", default="latest")
    shard.add_argument("--offset-start", type=int, required=True)
    shard.add_argument("--offset-end", type=int, required=True)
    shard.add_argument("--output", required=True)
    shard.add_argument("--seed-db", default="NeonTempo/SeedCatalog.sqlite")
    shard.add_argument("--token-file", default="apple_music_developer_token.txt")
    shard.add_argument("--temp-dir")
    shard.add_argument("--storefront", action="append")
    shard.add_argument("--drop-if-max-relevance-at-most", type=int, default=40)
    shard.add_argument("--minimum-storefront-relevance", type=int, default=50)
    shard.add_argument("--generated-at")
    shard.add_argument("--source-version")
    shard.add_argument("--resume", action="store_true")

    merge = subparsers.add_parser("merge")
    merge.add_argument("--shard-dir", required=True)
    merge.add_argument("--shard-glob", default="premium_v3_*.sqlite")
    merge.add_argument("--output", required=True)
    merge.add_argument("--report", default="logs/PremiumCatalog.v3.report.json")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "shard":
        shard_command(args)
    elif args.command == "merge":
        merge_command(args)
    else:
        raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
