#scripts/forex_factory_scraper.py
import os
import sys
import time
import random
import logging
import io
import re
from datetime import datetime, timedelta
import argparse
import demjson3
import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import hashlib
# === UTF-8 Console Fix (Windows) ===
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


    from datetime import datetime
# === Logging ===
logger = logging.getLogger("forex_factory_scraper")
logger.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("forex_factory_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)


# === Config ===
DATA_DIR = "calendar"
os.makedirs(DATA_DIR, exist_ok=True)

START_DATE = datetime(2024, 7, 1)
END_DATE = datetime(2025, 7, 11)
MAX_RETRIES = 3


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    return parser.parse_args()


# === Driver Setup ===
def random_user_agent():
    return random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0",
    ])

def create_driver() -> uc.Chrome:
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"user-agent={random_user_agent()}")
    return uc.Chrome(options=options, headless=False)


def build_week_url(date: datetime) -> str:
    return f"https://www.forexfactory.com/calendar?week={date.strftime('%b%d.%Y')}"

def extract_event_name(td) -> str:
    try:
        span = td.find("span", class_="calendar__event-title")
        return span.get_text(strip=True) if span else ""
    except:
        return ""

import re
import demjson3

def extract_calendar_json(html: str) -> dict:
    """
    Safely extract the JS object from the raw HTML content.
    """
    pattern = r"window\.calendarComponentStates\s*=\s*(\{.*?\});"
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        raise ValueError("❌ Could not find 'calendarComponentStates' in HTML.")

    js_data = match.group(1)

    try:
        return demjson3.decode(js_data)
    except demjson3.JSONDecodeError as e:
        debug_path = "debug/bad_calendar_json.txt"
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(js_data)
        raise ValueError(f"❌ Failed to parse calendar JSON: {e}")


def parse_calendar_html_json(data: dict) -> pd.DataFrame:
    cal_state = data.get("1", {})
    if "days" not in cal_state:
        raise RuntimeError("❌ Unexpected calendar structure in JSON.")
    events = []
    for day in cal_state["days"]:
        date_str = BeautifulSoup(day.get("date", ""), "html.parser").get_text(strip=True)
        for e in day.get("events", []):
            try:
                timestamp = datetime.fromtimestamp(e["dateline"])
                event_text = e.get("name", "").strip()
                currency = e.get("currency", "").strip()
                impact = e.get("impactTitle", "").strip()
                actual = e.get("actual", "").strip()
                forecast = e.get("forecast", "").strip()
                previous = e.get("previous", "").strip()

                fallback_event = f"actual:{actual}|forecast:{forecast}|previous:{previous}"
                id_input = f"{timestamp}{currency}{event_text or fallback_event}"
                event_id = hashlib.md5(id_input.encode()).hexdigest()

                events.append({
                    "timestamp": timestamp,
                    "currency": currency,
                    "impact": impact,
                    "event": event_text,
                    "actual": actual,
                    "forecast": forecast,
                    "previous": previous,
                    "day": date_str,
                    "event_id": event_id
                })
            except Exception as ex:
                logger.warning(f"[⚠ Skipping JSON event] {ex} → {e}")
    return pd.DataFrame(events)

def parse_calendar_time(week_date: datetime, day_str: str, time_str: str) -> datetime:
    try:
        # Add a space between weekday and month if missing: "MonJul" → "Mon Jul"
        if len(day_str) >= 6 and not day_str[3].isspace():
            day_str = f"{day_str[:3]} {day_str[3:]}"  # Fix "MonJul" → "Mon Jul"

        full_date = f"{day_str} {week_date.year} {time_str}"
        return datetime.strptime(full_date, "%a %b %d %Y %I:%M%p")
    except ValueError:
        # Handle "All Day" or blank times
        try:
            if len(day_str) >= 6 and not day_str[3].isspace():
                day_str = f"{day_str[:3]} {day_str[3:]}"
            return datetime.strptime(f"{day_str} {week_date.year}", "%a %b %d %Y")
        except Exception as e:
            raise RuntimeError(f"Failed to parse date: {day_str} {time_str} → {e}")


def parse_calendar_dom(html: str, week_date: datetime = None) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    events = []
    current_day = ""

    rows = soup.select("tr.calendar__row")
    for row in rows:
        try:
            # Get time and currency
            time_cell = row.select_one("td.calendar_cell.calendar_time")
            time_str = time_cell.get_text(strip=True) if time_cell else ""

            date_cell = row.select_one("td.calendar_cell.calendar_date")
            if date_cell:
                current_day = date_cell.get_text(strip=True)

            currency = row.select_one("td.calendar_cell.calendar_currency")
            currency = currency.get_text(strip=True) if currency else ""

            impact = row.select_one("td.calendar_cell.calendar_impact")
            impact = impact.get("title", "").strip() if impact else ""

            event_name = row.select_one("span.calendar__event-title")
            event_name = event_name.get_text(strip=True) if event_name else ""

            actual = row.select_one("td.calendar_cell.calendar_actual")
            actual = actual.get_text(strip=True) if actual else ""

            forecast = row.select_one("td.calendar_cell.calendar_forecast")
            forecast = forecast.get_text(strip=True) if forecast else ""

            previous = row.select_one("td.calendar_cell.calendar_previous")
            previous = previous.get_text(strip=True) if previous else ""

            # Combine date and time
            if not time_str or not current_day:
                continue  # skip empty rows

            timestamp = parse_calendar_time(week_date, current_day, time_str)

            # Generate unique event ID
            id_input = f"{timestamp}{currency}{event_name or actual+forecast+previous}"
            event_id = hashlib.md5(id_input.encode()).hexdigest()

            events.append({
                "timestamp": timestamp,
                "currency": currency,
                "impact": impact,
                "event": event_name,
                "actual": actual,
                "forecast": forecast,
                "previous": previous,
                "day": current_day,
                "event_id": event_id
            })
        except Exception as ex:
            logger.warning(f"[⚠ Skip row] {ex}")
            continue

    return pd.DataFrame(events)


def parse_calendar_html_fallback(html: str, week_date: datetime = None) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="calendar__table")
    if not table:
        raise RuntimeError("⚠ Could not find calendar table in HTML.")

    rows = []
    current_date = None

    for tr in table.find_all("tr"):
        date_td = tr.find("td", class_="calendar_cell calendar_date")
        time_td = tr.find("td", class_="calendar_cell calendar_time")
        currency_td = tr.find("td", class_="calendar_cell calendar_currency")
        impact_td = tr.find("td", class_="calendar_cell calendar_impact")
        event_td = tr.find("td", class_="calendar_cell calendar_event")
        actual_td = tr.find("td", class_="calendar_cell calendar_actual")
        forecast_td = tr.find("td", class_="calendar_cell calendar_forecast")
        previous_td = tr.find("td", class_="calendar_cell calendar_previous")

        if date_td and date_td.text.strip():
            current_date = date_td.text.strip()

        if not current_date or not currency_td:
            continue

        time_str = time_td.text.strip() if time_td else "12:00am"
        currency = currency_td.text.strip()

        # Try class or title fallback
        impact = ""
        if impact_td:
            impact_class = " ".join(impact_td.get("class", [])) + " "
            if "impact-high" in impact_class:
                impact = "High"
            elif "impact-medium" in impact_class:
                impact = "Medium"
            elif "impact-low" in impact_class:
                impact = "Low"
            elif (span := impact_td.find("span")) and span.get("title"):
                impact = span["title"].strip()

        event = extract_event_name(event_td)
        actual = actual_td.text.strip() if actual_td else ""
        forecast = forecast_td.text.strip() if forecast_td else ""
        previous = previous_td.text.strip() if previous_td else ""

        try:
            cleaned_date = re.sub(r"\s+", " ", current_date).strip()
            timestamp_str = f"{cleaned_date} {time_str} {week_date.year}"
            timestamp = datetime.strptime(timestamp_str, "%a %b %d %I:%M%p %Y")
        except Exception:
            timestamp = None

        id_input = f"{timestamp}{currency}{event or actual}"
        event_id = hashlib.md5(id_input.encode()).hexdigest()

        rows.append({
            "timestamp": timestamp,
            "currency": currency,
            "impact": impact,
            "event": event,
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "day": cleaned_date,
            "event_id": event_id
        })

    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["event_id"], inplace=True)
    return df[df["currency"].notnull()]

import js2py

def parse_calendar_html(html: str) -> pd.DataFrame:
    pattern = r"calendarComponentStates\s*\[\s*1\s*\]\s*=\s*(\{.*?\});"
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        raise ValueError("❌ Could not find 'calendarComponentStates[1]' in HTML.")

    js_data = match.group(1)

    # Save for debugging
    os.makedirs("debug", exist_ok=True)
    with open("debug/raw_calendar_json.js", "w", encoding="utf-8") as f:
        f.write(js_data)

    try:
        js_obj = js2py.eval_js(f"var data = {js_data}; data")
        calendar_data = js_obj.to_dict()  # 🔥 This is the fix
    except Exception as e:
        raise ValueError(f"❌ Failed to parse calendar JS with js2py: {e}")

    # 🔄 Replace .get() usage with proper Python dict access
    cal_state = calendar_data.get("days") or calendar_data.get("1", {}).get("days")

    if not cal_state:
        raise ValueError("❌ JSON structure invalid: no 'days' in calendarComponentStates[1]")

    # === STEP 2: Convert to DataFrame ===
    events = []
    for day in cal_state:
        date_str = BeautifulSoup(day.get("date", ""), "html.parser").get_text(strip=True)
        for event in day.get("events", []):
            try:
                ts = event.get("dateline")
                if not ts:
                    continue

                timestamp = datetime.fromtimestamp(ts)
                event_text = event.get("name", "").strip()
                currency = event.get("currency", "").strip()
                impact = event.get("impactTitle", "").strip()
                actual = event.get("actual", "").strip()
                forecast = event.get("forecast", "").strip()
                previous = event.get("previous", "").strip()

                id_input = f"{timestamp}{currency}{event_text}"
                event_id = hashlib.md5(id_input.encode()).hexdigest()

                events.append({
                    "timestamp": timestamp,
                    "currency": currency,
                    "impact": impact,
                    "event": event_text,
                    "actual": actual,
                    "forecast": forecast,
                    "previous": previous,
                    "day": date_str,
                    "event_id": event_id
                })
            except Exception as ex:
                logger.warning(f"[⚠ Skip event] {ex} → {event}")

    df = pd.DataFrame(events)
    return df

def scrape_week(driver, week_date: datetime):
    url = build_week_url(week_date)
    logger.info(f"🌐 Scraping Forex Factory for week: {week_date.strftime('%Y-%m-%d')}")

    for attempt in range(1, 4):
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "calendar__table"))
            )
            break
        except Exception as e:
            logger.warning(f"🔁 Retry {attempt} due to get() failure: {e}")
            time.sleep(random.uniform(3, 6))
    else:
        raise RuntimeError(f"🚫 Failed to load {url} after retries.")

    html = driver.page_source

    # Save raw HTML for debugging
    os.makedirs("debug", exist_ok=True)
    week_dt = week_date if isinstance(week_date, datetime) else datetime.combine(week_date, datetime.min.time())
    debug_html_path = f"debug/raw_html_{week_dt.strftime('%Y%m%d')}.html"
    with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)

    try:
        df = parse_calendar_html(html)

        if df.empty or "timestamp" not in df.columns:
            raise ValueError("❌ No valid 'timestamp' in parsed calendar data.")

        df = df[df["timestamp"].notnull()]
        if df.empty:
            logger.warning(f"⚠ No events with timestamp for week of {week_date.strftime('%Y-%m-%d')}")
            return

        # === Save to CSV ===
        filename = f"{DATA_DIR}/week_{week_date.strftime('%Y%m%d')}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"✅ Saved {len(df)} events → {filename}")

    except Exception as e:
        debug_path = f"debug/ff_calendar_{week_dt.strftime('%Y%m%d')}.html"
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(html)
        raise RuntimeError(f"❌ Failed to parse calendar for {week_date.date()}: {e}. Saved HTML to {debug_path}")

def merge_all_weeks():
    logger.info("🔄 Merging all weekly CSVs...")
    files = sorted([
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".csv") and f.startswith("week_")
    ])
    if not files:
        logger.warning("⚠ No CSVs found to merge.")
        return

    combined_df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    combined_df.sort_values("timestamp", inplace=True)
    output_file = os.path.join(DATA_DIR, "fx_news.csv")
    combined_df.to_csv(output_file, index=False)
    logger.info(f"🧾 Merged {len(files)} files into {output_file}")

import argparse
import os
import time
import random
from datetime import datetime, timedelta
# Assume other necessary imports (create_driver, scrape_week, logger, etc.) are already above

# === Constants ===
MAX_RETRIES = 3
DATA_DIR = "calendar"
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime.utcnow()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--output", type=str, default="calendar/fx_news.csv", help="Path to save scraped news CSV")
    return parser.parse_args()


def main():
    args = parse_args()
    
    start = datetime.strptime(args.start, "%Y-%m-%d") if args.start else START_DATE
    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else END_DATE
    current = start

    driver = create_driver()

    while current <= end:
        csv_path = os.path.join(DATA_DIR, f"week_{current.strftime('%Y%m%d')}.csv")
        if os.path.exists(csv_path):
            logger.info(f"📁 Already scraped: {csv_path}")
            current += timedelta(days=7)
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                scrape_week(driver, current)
                break
            except Exception as e:
                logger.error(f"❌ Attempt {attempt} failed for {current.date()}: {type(e).__name__} - {e}")
                if attempt < MAX_RETRIES:
                    logger.info("🔄 Restarting driver...")
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = create_driver()
                    time.sleep(random.uniform(3, 6))
        else:
            logger.error(f"🚫 Failed all {MAX_RETRIES} retries for week {current.date()}")

        current += timedelta(days=7)
        time.sleep(random.uniform(2, 4))

    try:
        driver.quit()
    except:
        pass

    merge_all_weeks()
    logger.info("🎉 All weeks completed.")
    logger.info(f"📁 Check your data in: {os.path.abspath(DATA_DIR)}")


if __name__ == "__main__":
    main()
