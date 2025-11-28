#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script Name : step2_enrich_with_preferred_channels.py
Purpose     : Enriches all_sites_master_channels.csv with preferred channel metadata
              from prefered-scoped-channels.csv and optional exclude_categories.csv.
Author      : Andrew J. Pearen + ChatGPT co-pilot
Version     : v1.0.0
Created     : 2025-11-24
Last Update : 2025-11-24

Execution   :
    Windows PowerShell:
        python C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master\custom\scripts\step2_enrich_with_preferred_channels.py

Inputs      :
    - custom/baseline/all_sites_master_channels.csv
    - custom/rules/prefered-scoped-channels.csv
    - custom/rules/exclude_categories.csv  (optional)

Outputs     :
    - custom/baseline/all_sites_master_channels_enriched.csv
    - custom/baseline/versioned/all_sites_master_channels_enriched_YYYYMMDD_HHMMSS.csv
    - Logs written to:
        custom/logs/step2_enrich_with_preferred_channels.log
"""

import os
import sys
import csv
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import re

try:
    import pandas as pd
except ImportError as exc:
    print("[FATAL] pandas is required for this script. Install it with 'pip install pandas'.")
    raise


# ---------------------------------------------------------------------------
# Path / environment helpers
# ---------------------------------------------------------------------------

SCRIPT_NAME = "step2_enrich_with_preferred_channels.py"
SCRIPT_VERSION = "v1.0.0"


def get_base_path() -> str:
    """
    Determine the base path for the repo:
        <repo_root> = .../AJPs-custom-epg-master/AJPs-custom-epg-master

    This script is expected to live in:
        <repo_root>/custom/scripts/step2_enrich_with_preferred_channels.py
    """
    here = os.path.abspath(os.path.dirname(__file__))
    # up two levels: .../custom/scripts -> .../custom -> .../<repo_root>
    base_path = os.path.abspath(os.path.join(here, os.pardir, os.pardir))
    return base_path


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logger(base_path: str) -> logging.Logger:
    """
    Configure a logger that writes to both console and a log file.

    Log file:
        <base_path>/custom/logs/step2_enrich_with_preferred_channels.log
    """
    logs_dir = os.path.join(base_path, "custom", "logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, "step2_enrich_with_preferred_channels.log")

    logger = logging.getLogger("step2_enrich")
    # Avoid duplicate handlers if script imported/re-run
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Console handler (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_fmt = logging.Formatter(
        fmt="[%(asctime)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch.setFormatter(ch_fmt)

    # File handler (DEBUG+)
    fh = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh_fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fh_fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("Logger initialized. Log file: %s", log_path)
    return logger


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """
    Normalize a channel name for fuzzy matching:
      - Lowercase
      - Remove non-alphanumeric characters
      - Collapse whitespace
    """
    if not isinstance(name, str):
        return ""
    s = name.strip().lower()
    # Remove 'hd', '+1' only when they are suffixes; leave core name
    # but still strip punctuation generally
    s = re.sub(r"\bhd\b", "", s)
    s = re.sub(r"\+1\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def load_csv_safe(path: str, logger: logging.Logger, desc: str) -> pd.DataFrame:
    """
    Load a CSV into a DataFrame with standard options and log details.
    """
    if not os.path.exists(path):
        logger.error("Required %s file not found: %s", desc, path)
        raise FileNotFoundError(f"{desc} file not found: {path}")

    logger.info("Loading %s from %s", desc, path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    logger.info("%s rows loaded from %s", len(df), path)
    return df


def load_exclude_categories(path: str, logger: logging.Logger) -> set:
    """
    Load exclude_categories.csv if present. Expected formats:

    Option A (preferred):
        category
        music
        shopping

    Option B (fallback):
        single column, header unknown -> use first column.

    Returns:
        A set of lowercase category names.
    """
    if not os.path.exists(path):
        logger.info("No exclude_categories.csv found at %s; skipping category exclusion.", path)
        return set()

    logger.info("Loading exclude categories from %s", path)

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        categories = set()

        if "category" in [fn.lower() for fn in fieldnames]:
            # Find the actual field name case
            cat_field = next(fn for fn in fieldnames if fn.lower() == "category")
            for row in reader:
                val = (row.get(cat_field) or "").strip().lower()
                if val:
                    categories.add(val)
        else:
            # Fallback: use the first column as category
            first_field = fieldnames[0]
            for row in reader:
                val = (row.get(first_field) or "").strip().lower()
                if val:
                    categories.add(val)

    logger.info("Loaded %d exclude categories.", len(categories))
    if categories:
        logger.debug("Exclude categories: %s", sorted(categories))
    return categories


# ---------------------------------------------------------------------------
# Core enrichment logic
# ---------------------------------------------------------------------------

def build_preferred_lookup(preferred_df: pd.DataFrame, logger: logging.Logger):
    """
    Build two lookups from preferred_df:

    1) by id (exact)
    2) by (normalized_name, country) from name + alt_names

    Returns:
        (by_id: dict, by_name_country: dict)
    """
    required_cols = ["id", "name"]
    missing = [c for c in required_cols if c not in preferred_df.columns]
    if missing:
        logger.error("Preferred CSV missing required columns: %s", ", ".join(missing))
        raise ValueError("Invalid prefered-scoped-channels.csv; missing required columns.")

    by_id = {}
    by_name_country = {}

    for _, row in preferred_df.iterrows():
        cid = (row.get("id") or "").strip()
        if cid:
            if cid in by_id:
                logger.debug("Duplicate preferred id encountered: %s (keeping first)", cid)
            else:
                by_id[cid] = row

        # Build name-based keys
        country = (row.get("country") or "").strip().upper()
        name_main = row.get("name") or ""
        alt_names = row.get("alt_names") or ""

        names = [name_main]
        if alt_names:
            for part in str(alt_names).split(";"):
                nm = part.strip()
                if nm:
                    names.append(nm)

        for nm in names:
            norm = normalize_name(nm)
            if not norm:
                continue
            key = (norm, country or None)
            if key not in by_name_country:
                by_name_country[key] = row
            else:
                # Avoid log spam but keep awareness
                logger.debug("Collision on name-country key %s; keeping first", key)

    logger.info("Preferred lookup built: %d entries by id, %d entries by name+country",
                len(by_id), len(by_name_country))
    return by_id, by_name_country


def enrich_baseline_with_preferred(
    baseline_df: pd.DataFrame,
    preferred_df: pd.DataFrame,
    exclude_categories: set,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Enrich baseline dataframe with preferred metadata and category exclusion flags.
    """

    # QA: ensure we have minimal columns for baseline
    if "xmltv_id" not in baseline_df.columns and "display_name" not in baseline_df.columns:
        logger.error("Baseline CSV must contain at least 'xmltv_id' or 'display_name' column.")
        raise ValueError("Baseline CSV missing both 'xmltv_id' and 'display_name' columns.")

    # Ensure columns we will add exist
    add_cols = [
        "preferred_flag",
        "pref_id",
        "pref_name",
        "pref_alt_names",
        "pref_network",
        "pref_owners",
        "pref_country",
        "pref_categories",
        "pref_is_nsfw",
        "pref_launched",
        "pref_closed",
        "pref_replaced_by",
        "pref_website",
        "filtered_out_by_category",
    ]
    for col in add_cols:
        if col not in baseline_df.columns:
            baseline_df[col] = ""

    by_id, by_name_country = build_preferred_lookup(preferred_df, logger)

    preferred_matches_id = 0
    preferred_matches_name = 0
    unmatched = 0
    filtered_by_category = 0

    # Precompute the set of preferred ids for quick flagging if "Prefered" column missing
    preferred_ids_set = set((preferred_df.get("id") or []))

    # Iterate row-wise
    for idx, row in baseline_df.iterrows():
        matched_row = None

        # 1) try id-based match
        xmltv_id = (row.get("xmltv_id") or "").strip()
        if xmltv_id and xmltv_id in by_id:
            matched_row = by_id[xmltv_id]
            preferred_matches_id += 1
        else:
            # 2) try name-based match
            # Country from baseline (if available)
            b_country = (row.get("country") or "").strip().upper()
            display_name = row.get("display_name") or row.get("name") or ""
            norm_name = normalize_name(display_name)
            key_exact = (norm_name, b_country or None)
            key_nocountry = (norm_name, None)

            if key_exact in by_name_country:
                matched_row = by_name_country[key_exact]
                preferred_matches_name += 1
            elif key_nocountry in by_name_country:
                matched_row = by_name_country[key_nocountry]
                preferred_matches_name += 1
            else:
                unmatched += 1

        if matched_row is not None:
            # Fill preferred metadata
            cid = (matched_row.get("id") or "").strip()
            baseline_df.at[idx, "pref_id"] = cid
            baseline_df.at[idx, "pref_name"] = matched_row.get("name") or ""
            baseline_df.at[idx, "pref_alt_names"] = matched_row.get("alt_names") or ""
            baseline_df.at[idx, "pref_network"] = matched_row.get("network") or ""
            baseline_df.at[idx, "pref_owners"] = matched_row.get("owners") or ""
            baseline_df.at[idx, "pref_country"] = (matched_row.get("country") or "").strip().upper()
            baseline_df.at[idx, "pref_categories"] = matched_row.get("categories") or ""
            baseline_df.at[idx, "pref_is_nsfw"] = matched_row.get("is_nsfw") or ""
            baseline_df.at[idx, "pref_launched"] = matched_row.get("launched") or ""
            baseline_df.at[idx, "pref_closed"] = matched_row.get("closed") or ""
            baseline_df.at[idx, "pref_replaced_by"] = matched_row.get("replaced_by") or ""
            baseline_df.at[idx, "pref_website"] = matched_row.get("website") or ""

            # Determine preferred_flag
            prefered_col_value = (matched_row.get("Prefered") or matched_row.get("preferred") or "").strip()
            if prefered_col_value:
                baseline_df.at[idx, "preferred_flag"] = "Y" if prefered_col_value.upper().startswith("Y") else ""
            else:
                # If Prefered column is absent or empty, but it is in the preferred list at all, treat as preferred.
                baseline_df.at[idx, "preferred_flag"] = "Y" if cid in preferred_ids_set else ""
        else:
            # leave metadata empty
            baseline_df.at[idx, "preferred_flag"] = baseline_df.at[idx, "preferred_flag"] or ""

        # Category-based filtering
        # Category field priority: pref_categories -> baseline categories (if exists)
        cat_field = ""
        if baseline_df.at[idx, "pref_categories"]:
            cat_field = baseline_df.at[idx, "pref_categories"]
        else:
            if "categories" in baseline_df.columns:
                cat_field = row.get("categories") or ""

        cat_list = [c.strip().lower() for c in str(cat_field).split(";") if c.strip()]

        # Only filter if we actually have exclude_categories defined
        if exclude_categories and cat_list:
            if any(c in exclude_categories for c in cat_list):
                # Do not filter out channels explicitly marked as preferred
                if baseline_df.at[idx, "preferred_flag"] != "Y":
                    baseline_df.at[idx, "filtered_out_by_category"] = "Y"
                    filtered_by_category += 1

    logger.info("Preferred matches by id   : %d", preferred_matches_id)
    logger.info("Preferred matches by name : %d", preferred_matches_name)
    logger.info("Baseline rows unmatched   : %d", unmatched)
    logger.info("Rows flagged by category  : %d", filtered_by_category)

    return baseline_df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    base_path = get_base_path()
    logger = setup_logger(base_path)

    logger.info("=== %s %s starting ===", SCRIPT_NAME, SCRIPT_VERSION)
    logger.info("BasePath   : %s", base_path)

    baseline_path = os.path.join(base_path, "custom", "baseline", "all_sites_master_channels.csv")
    preferred_path = os.path.join(base_path, "custom", "rules", "prefered-scoped-channels.csv")
    exclude_categories_path = os.path.join(base_path, "custom", "rules", "exclude_categories.csv")

    # Output paths
    baseline_dir = os.path.join(base_path, "custom", "baseline")
    versioned_dir = os.path.join(baseline_dir, "versioned")
    os.makedirs(baseline_dir, exist_ok=True)
    os.makedirs(versioned_dir, exist_ok=True)

    enriched_path = os.path.join(baseline_dir, "all_sites_master_channels_enriched.csv")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    enriched_versioned_path = os.path.join(
        versioned_dir,
        f"all_sites_master_channels_enriched_{ts}.csv",
    )

    try:
        baseline_df = load_csv_safe(baseline_path, logger, "baseline (Step 1 output)")
        preferred_df = load_csv_safe(preferred_path, logger, "preferred channels")
        exclude_categories = load_exclude_categories(exclude_categories_path, logger)
    except Exception as exc:
        logger.exception("Failed during input loading: %s", exc)
        sys.exit(1)

    try:
        enriched_df = enrich_baseline_with_preferred(
            baseline_df=baseline_df,
            preferred_df=preferred_df,
            exclude_categories=exclude_categories,
            logger=logger,
        )
    except Exception as exc:
        logger.exception("Failed during enrichment: %s", exc)
        sys.exit(1)

    # Save outputs
    try:
        enriched_df.to_csv(enriched_path, index=False, encoding="utf-8")
        logger.info("Wrote enriched baseline to %s", enriched_path)

        enriched_df.to_csv(enriched_versioned_path, index=False, encoding="utf-8")
        logger.info("Wrote versioned enriched baseline to %s", enriched_versioned_path)

    except Exception as exc:
        logger.exception("Failed while writing enriched outputs: %s", exc)
        sys.exit(1)

    logger.info("=== %s %s completed successfully ===", SCRIPT_NAME, SCRIPT_VERSION)


if __name__ == "__main__":
    main()
