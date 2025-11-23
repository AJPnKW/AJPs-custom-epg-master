#!/usr/bin/env python3
"""
Script: merge_custom_channels.py
Purpose: Merge Draft-Keep, matched, and recommended channel lists into a single custom.channels.xml
Author: ChatGPT (for Andrew)
Created: 2025-11-22
Version: 1.0

Usage:
  python merge_custom_channels.py

Expected inputs (edit paths below if needed):
  custom/output/Draft-Keep.csv
  custom/output/matched_channels.csv
  custom/output/recommended_custom_list.csv
  custom/output/custom.channels.xml   (current file)

Outputs:
  custom/output/custom.channels.merged.xml
  custom/output/merge_review_duplicates.csv
  custom/output/merge_review_unmatched.csv
Logs:
  custom/logs/merge_custom_channels.log
"""

import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from difflib import SequenceMatcher

# ---------------------------
# PATHS (edit if you want)
# ---------------------------
BASE = r"C:\Users\Lenovo\PROJECTS\AJPs-custom-epg-master\AJPs-custom-epg-master"
IN_DIR = os.path.join(BASE, "custom", "output")
LOG_DIR = os.path.join(BASE, "custom", "logs")
OUT_DIR = os.path.join(BASE, "custom", "output")

DRAFT_KEEP = os.path.join(IN_DIR, "Draft-Keep.csv")
MATCHED = os.path.join(IN_DIR, "matched_channels.csv")
RECOMMENDED = os.path.join(IN_DIR, "recommended_custom_list.csv")
CUSTOM_XML = os.path.join(IN_DIR, "custom.channels.xml")

OUT_XML = os.path.join(OUT_DIR, "custom.channels.merged.xml")
OUT_DUPES = os.path.join(OUT_DIR, "merge_review_duplicates.csv")
OUT_UNMATCHED = os.path.join(OUT_DIR, "merge_review_unmatched.csv")
LOG_FILE = os.path.join(LOG_DIR, "merge_custom_channels.log")

# ---------------------------
# LOGGING
# ---------------------------
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(f"[{ts}] {msg}")

# ---------------------------
# NORMALIZATION
# ---------------------------
QUALITY_TAGS = ["hd", "uhd", "4k", "fhd", "sd"]
PROVIDER_TAGS = [
    "directv", "pluto", "plex", "freeview", "sky", "virgin", "tvguide",
    "tvinsider", "tv24", "streaming", "epgshare"
]
TIMESHIFT_TAGS = ["+1", "+2", "timeshift", "east", "west"]

def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()

    # remove provider prefixes anywhere
    for p in PROVIDER_TAGS:
        n = re.sub(rf"\b{re.escape(p)}\b", " ", n)

    # remove quality tags
    for q in QUALITY_TAGS:
        n = re.sub(rf"\b{re.escape(q)}\b", " ", n)

    # remove punctuation
    n = re.sub(r"[\(\)\[\]\-_/&,+.']", " ", n)

    # collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()

    return n

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# ---------------------------
# LOAD CSVs
# ---------------------------
def load_csv(path):
    rows = []
    if not os.path.exists(path):
        log(f"WARNING missing file: {path}")
        return rows

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    log(f"Loaded {len(rows)} rows from {os.path.basename(path)}")
    return rows

# ---------------------------
# LOAD EXISTING custom.channels.xml
# ---------------------------
def load_custom_xml(path):
    items = []
    if not os.path.exists(path):
        log(f"WARNING missing file: {path}")
        return items

    tree = ET.parse(path)
    root = tree.getroot()
    for ch in root.findall("channel"):
        items.append({
            "site": ch.attrib.get("site", "").strip(),
            "xmltv_id": ch.attrib.get("xmltv_id", "").strip(),
            "name": (ch.text or "").strip()
        })
    log(f"Loaded {len(items)} channels from custom.channels.xml")
    return items

# ---------------------------
# BUILD INDEX FROM ALL SOURCES
# ---------------------------
def build_candidates():
    candidates = []

    # Current custom list (highest priority)
    for r in load_custom_xml(CUSTOM_XML):
        r["source"] = "custom.channels.xml"
        candidates.append(r)

    # CSV lists (next priority)
    for src_name, path in [
        ("Draft-Keep.csv", DRAFT_KEEP),
        ("matched_channels.csv", MATCHED),
        ("recommended_custom_list.csv", RECOMMENDED)
    ]:
        for r in load_csv(path):
            # try to map common column names
            name = r.get("display_name") or r.get("name") or r.get("channel") or r.get("title") or ""
            site = r.get("site") or r.get("source_site") or ""
            xmltv_id = r.get("xmltv_id") or r.get("id") or ""
            candidates.append({
                "site": site,
                "xmltv_id": xmltv_id,
                "name": name,
                "source": src_name
            })

    log(f"Total candidates before dedupe: {len(candidates)}")
    return candidates

# ---------------------------
# DEDUPE + MERGE
# ---------------------------
def merge_candidates(candidates):
    merged = {}
    dupes = []
    unmatched = []

    for c in candidates:
        name = c["name"]
        site = c["site"]
        xmltv_id = c["xmltv_id"]

        norm = normalize_name(name)

        # If no useful name, skip to unmatched
        if not norm:
            unmatched.append(c)
            continue

        # Primary key preference:
        # 1) xmltv_id if present
        # 2) normalized name
        key = xmltv_id if xmltv_id else norm

        if key in merged:
            # decide which one wins
            prev = merged[key]
            winner = prev

            # prefer having xmltv_id
            if (not prev["xmltv_id"]) and xmltv_id:
                winner = c
            # else prefer custom list source
            elif prev["source"] != "custom.channels.xml" and c["source"] == "custom.channels.xml":
                winner = c

            if winner is c:
                merged[key] = c

            dupes.append({
                "key": key,
                "kept_name": merged[key]["name"],
                "dropped_name": name,
                "kept_source": merged[key]["source"],
                "dropped_source": c["source"],
                "kept_xmltv_id": merged[key]["xmltv_id"],
                "dropped_xmltv_id": xmltv_id,
            })
        else:
            merged[key] = c

    log(f"Merged unique channels: {len(merged)}")
    log(f"Potential duplicates collapsed: {len(dupes)}")
    log(f"Unmatched/empty-name rows: {len(unmatched)}")

    return list(merged.values()), dupes, unmatched

# ---------------------------
# WRITE OUTPUTS
# ---------------------------
def write_xml(channels, out_path):
    root = ET.Element("channels")
    for c in sorted(channels, key=lambda x: (x["site"], x["name"].lower())):
        attrs = {}
        if c["site"]:
            attrs["site"] = c["site"]
        if c["xmltv_id"]:
            attrs["xmltv_id"] = c["xmltv_id"]
        ch_el = ET.SubElement(root, "channel", attrib=attrs)
        ch_el.text = c["name"]

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ", level=0)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)
    log(f"Wrote merged XML: {out_path}")

def write_csv(rows, out_path, fieldnames):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    log(f"Wrote CSV: {out_path}")

# ---------------------------
# MAIN
# ---------------------------
def main():
    log("Starting merge_custom_channels.py")
    os.makedirs(OUT_DIR, exist_ok=True)

    candidates = build_candidates()
    merged, dupes, unmatched = merge_candidates(candidates)

    write_xml(merged, OUT_XML)

    if dupes:
        write_csv(
            dupes, OUT_DUPES,
            ["key","kept_name","dropped_name","kept_source","dropped_source","kept_xmltv_id","dropped_xmltv_id"]
        )

    if unmatched:
        write_csv(
            unmatched, OUT_UNMATCHED,
            ["site","xmltv_id","name","source"]
        )

    log("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
        sys.exit(1)
