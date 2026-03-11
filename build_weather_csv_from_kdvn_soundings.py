#!/usr/bin/env python3
"""
Builds a CSV in the Analyzer-required format using University of Wyoming
Upper-Air soundings for KDVN (WMO 74455) between a date range.

Output format (no header):
    unix_timestamp_seconds, elevation_m_AGL, potential_temperature_K, wind_speed_m_s

Notes:
- Data are observed radiosonde soundings from the nearest upper-air site
  to Cedar Rapids, IA (KDVN / WMO 74455).
- Script tries the newer WSGI CSV endpoint first, then falls back to the
  legacy CGI TEXT:LIST endpoint if necessary.
- Potential temperature is computed as theta = (T_C + 273.15) * (1000.0 / P_hPa) ** 0.286
- Elevation AGL is computed as HGHT_m - min(HGHT_m) for each sounding time.
- Wind speed is converted to m/s when legacy output provides knots.

Usage examples:
  python build_weather_csv_from_kdvn_soundings.py \
      --start 2020-01-01 --end 2025-01-01 \
      --outfile "exhaust plume weather data - lat 41.884694 lon -91.710806 - 2020-01-01 thru 2025-01-01.csv"

Optional:
  --hours 00 12   (default)
  --station 74455 (WMO ID for KDVN)
"""

import sys
import argparse
from datetime import datetime, timedelta, timezone
import urllib.request
import urllib.error
import io
import csv
import re

DEFAULT_STATION = "74455"  # KDVN (Davenport, IA)

WSGI_URL_TMPL = (
    "https://weather.uwyo.edu/wsgi/sounding?datetime={dt}&id={st}&type=TEXT:CSV&src={src}"
)
CGI_URL_TMPL = (
    "http://weather.uwyo.edu/cgi-bin/sounding?TYPE=TEXT:LIST&YEAR={year}&MONTH={month}&FROM={from_}&TO={to_}&STNM={st}"
)


def fetch_text(url: str) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
        return data.decode("utf-8", errors="replace")
    except urllib.error.HTTPError:
        return None
    except urllib.error.URLError:
        return None


def theta_from_T_P(temp_c: float, pres_hpa: float) -> float:
    """Compute potential temperature (K) from temperature (C) and pressure (hPa)."""
    T_k = temp_c + 273.15
    if pres_hpa <= 0:
        return float("nan")
    return T_k * (1000.0 / pres_hpa) ** 0.286


def parse_wsgi_csv(text: str):
    """Parse Wyoming WSGI CSV text and return list of rows dicts.
       Expected header includes at least: pressure_hPa, geopotential height_m, temperature_C, wind speed_m/s
    """
    # Some responses can be empty or not CSV; detect by header keywords
    header_line = None
    for line in text.splitlines():
        if "," in line and "pressure_hPa" in line:
            header_line = line
            break
    if not header_line:
        return []

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        rows.append(row)
    return rows


def parse_legacy_textlist(text: str):
    """Parse legacy TEXT:LIST output (fixed-width-like text) into row dicts.
       We expect a section with columns including PRES, HGHT, TEMP, SKNT, and possibly THTA.
    """
    lines = text.splitlines()
    # Find header and divider lines
    start_idx = None
    end_idx = None
    # The data block is typically between dashed lines and ends before a blank line or next section.
    dash_lines = [i for i, line in enumerate(lines) if re.match(r"^-{5,}$", line.strip())]
    if len(dash_lines) >= 2:
        start_idx = dash_lines[1] + 1  # after second dashed line
    else:
        return [], None

    # Determine where data ends: next blank line or end of file
    for j in range(start_idx, len(lines)):
        if lines[j].strip() == "":
            end_idx = j
            break
    if end_idx is None:
        end_idx = len(lines)

    # Determine units for wind speed by scanning header text
    header_text = "\n".join(lines[:start_idx])
    wind_in_knots = "knot" in header_text.lower()

    # Column names order from header line if present
    colnames = None
    for k in range(start_idx - 1, -1, -1):
        if "PRES" in lines[k] and "HGHT" in lines[k]:
            colnames = re.findall(r"[A-Z]+", lines[k])
            break
    if not colnames:
        colnames = ["PRES", "HGHT", "TEMP", "DWPT", "RELH", "MIXR", "DRCT", "SKNT", "THTA", "THTE", "THTV"]

    rows = []
    for line in lines[start_idx:end_idx]:
        if not line.strip():
            continue
        parts = re.split(r"\s+", line.strip())
        if len(parts) < 8:
            continue
        # Map available parts to column names
        row = {}
        for idx, val in enumerate(parts):
            if idx >= len(colnames):
                break
            row[colnames[idx]] = val
        # Convert types
        def to_float(s):
            try:
                return float(s)
            except Exception:
                return float("nan")
        out = {
            "pressure_hPa": to_float(row.get("PRES", "nan")),
            "geopotential height_m": to_float(row.get("HGHT", "nan")),
            "temperature_C": to_float(row.get("TEMP", "nan")),
            "wind speed": to_float(row.get("SKNT", "nan")),
            "theta_K": to_float(row.get("THTA", "nan")),
        }
        if any(v != v for v in [out["pressure_hPa"], out["geopotential height_m"], out["temperature_C"]]):
            continue
        # Wind speed units: convert knots -> m/s if needed
        if wind_in_knots and out["wind speed"] == out["wind speed"]:
            out["wind speed_m_s"] = out["wind speed"] * 0.514444
        else:
            out["wind speed_m_s"] = out["wind speed"]
        rows.append(out)
    return rows, header_text


def build_times(start_date: datetime, end_date: datetime, hours: list[str]):
    """Generate datetimes (UTC) for each day between start (inclusive) and end (inclusive for 00Z)."""
    tz_utc = timezone.utc
    current = datetime(start_date.year, start_date.month, start_date.day, tzinfo=tz_utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, tzinfo=tz_utc)
    times = []
    one_day = timedelta(days=1)
    while current <= end_dt:
        for hh in hours:
            hour = int(hh)
            dt = current.replace(hour=hour, minute=0, second=0, microsecond=0)
            if dt > end_dt.replace(hour=23):
                continue
            times.append(dt)
        current += one_day
    return sorted(times)


def main():
    ap = argparse.ArgumentParser(description="Build weather CSV for Analyzer from KDVN soundings (UWYO)")
    ap.add_argument("--start", required=True, help="Start date (UTC) YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="End date (UTC) YYYY-MM-DD (inclusive; 00Z included)")
    ap.add_argument("--outfile", required=True, help="Output CSV file path (no header)")
    ap.add_argument("--station", default=DEFAULT_STATION, help="WMO station number (default 74455 KDVN)")
    ap.add_argument("--hours", nargs="*", default=["00", "12"], help="UTC hours to fetch each day (e.g., 00 12)")
    args = ap.parse_args()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        print("ERROR: Use YYYY-MM-DD for --start and --end", file=sys.stderr)
        sys.exit(1)

    target_times = build_times(start_date, end_date, args.hours)
    print(f"Planned soundings: {len(target_times)} times")

    total_rows = 0
    with open(args.outfile, "w", newline="") as fout:
        writer = csv.writer(fout)
        for dt in target_times:
            dt_str = dt.strftime("%Y-%m-%d %H:00:00")
            epoch = int(dt.timestamp())

            # Try WSGI BUFR first, then TEMP
            rows = []
            for src in ("BUFR", "TEMP"):
                url = WSGI_URL_TMPL.format(dt=urllib.parse.quote(dt_str), st=args.station, src=src)
                text = fetch_text(url)
                if text:
                    wsgi_rows = parse_wsgi_csv(text)
                    if wsgi_rows:
                        rows = []
                        for r in wsgi_rows:
                            try:
                                pres = float(r.get("pressure_hPa"))
                                hgt = float(r.get("geopotential height_m"))
                                temp_c = float(r.get("temperature_C"))
                                wspd = r.get("wind speed_m/s") or r.get("wind speed (m/s)") or r.get("wind speed")
                                wspd = float(wspd) if wspd is not None else float('nan')
                            except Exception:
                                continue
                            if any(v != v for v in [pres, hgt, temp_c]):
                                continue
                            theta_k = theta_from_T_P(temp_c, pres)
                            rows.append({
                                "pressure_hPa": pres,
                                "HGHT_m": hgt,
                                "theta_K": theta_k,
                                "wind_m_s": wspd,
                            })
                        if rows:
                            break
            # If still empty, fallback to legacy CGI textual list
            if not rows:
                year = dt.strftime("%Y")
                month = dt.strftime("%m")
                ddhh = dt.strftime("%d%H")
                url = CGI_URL_TMPL.format(year=year, month=month, from_=ddhh, to_=ddhh, st=args.station)
                text = fetch_text(url)
                if text:
                    legacy_rows, _ = parse_legacy_textlist(text)
                    rows = []
                    for r in legacy_rows:
                        pres = r.get("pressure_hPa")
                        hgt = r.get("geopotential height_m")
                        temp_c = r.get("temperature_C")
                        wspd = r.get("wind speed_m_s")
                        if any(v != v for v in [pres, hgt, temp_c]):
                            continue
                        theta_k = r.get("theta_K")
                        if not (theta_k == theta_k):  # NaN
                            theta_k = theta_from_T_P(temp_c, pres)
                        rows.append({
                            "pressure_hPa": pres,
                            "HGHT_m": hgt,
                            "theta_K": theta_k,
                            "wind_m_s": wspd if wspd == wspd else float("nan"),
                        })
            if not rows:
                print(f"{dt_str}Z: No data")
                continue
            # Compute AGL
            valid_hgts = [r["HGHT_m"] for r in rows if r["HGHT_m"] == r["HGHT_m"]]
            if not valid_hgts:
                print(f"{dt_str}Z: No valid heights")
                continue
            min_h = min(valid_hgts)
            # Sort by height ascending
            rows.sort(key=lambda x: x["HGHT_m"])
            # Write rows
            wcount = 0
            for r in rows:
                elev_agl = r["HGHT_m"] - min_h
                theta_k = r["theta_K"]
                wspd = r["wind_m_s"]
                if any(v != v for v in [elev_agl, theta_k, wspd]):
                    continue
                writer.writerow([epoch, f"{elev_agl:.6f}", f"{theta_k:.6f}", f"{wspd:.6f}"])
                wcount += 1
            total_rows += wcount
            print(f"{dt_str}Z: wrote {wcount} levels")

    print(f"Done. Total rows written: {total_rows}")


if __name__ == "__main__":
    main()
