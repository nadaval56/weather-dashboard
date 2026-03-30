#!/usr/bin/env python3
"""
Weather Dashboard Data Fetcher
מושך נתונים מ-FieldClimate ומעבד אותם לדשבורד
"""

import requests
import hmac
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import os

# ===== הגדרות =====
STATION_ID = os.environ.get('STATION_ID', '03114DE5')
PUBLIC_KEY = os.environ.get('PUBLIC_KEY')
PRIVATE_KEY = os.environ.get('PRIVATE_KEY')

if not PUBLIC_KEY or not PRIVATE_KEY:
    print("⚠️  אזהרה: לא נמצאו מפתחות API ב-environment variables")

API_BASE = "https://api.fieldclimate.com/v2"

# גשם שנרשם לפני הקמת התחנה
PRE_STATION_RAIN = 25.0

# שעון ישראל — אוטומטי, כולל שעון קיץ/חורף
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

def israel_now():
    return datetime.now(ISRAEL_TZ)

def israel_today():
    return israel_now().strftime('%Y-%m-%d')

def make_request(path):
    """שליחת בקשה ל-API עם אימות HMAC"""
    url = f"{API_BASE}{path}"
    timestamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

    content_to_sign = f"GET{path}{timestamp}{PUBLIC_KEY}"
    signature = hmac.new(
        PRIVATE_KEY.encode('utf-8'),
        content_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'Accept': 'application/json',
        'Authorization': f'hmac {PUBLIC_KEY}:{signature}',
        'Request-Date': timestamp
    }

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def load_season_data():
    try:
        with open('rain_season.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("📝 יצירת קובץ עונתי ראשוני...")
        return {
            "season_start": "2025-10-01",
            "legacy_season_rain": 0.0,
            "daily_rain": {}
        }

def save_season_data(season_data):
    with open('rain_season.json', 'w', encoding='utf-8') as f:
        json.dump(season_data, f, ensure_ascii=False, indent=2)

def calc_rain_from_raw(dates, rain_sensor):
    """
    חישוב גשם יומי ישירות מנתוני raw לפי timestamp.

    הלוגיקה:
      • כל דגימת raw יש לה timestamp בזמן ישראל
      • מחברים את כל הדגימות לפי תאריך
      • לא תלויים באיפוס של FieldClimate בחצות

    כך גשם שירד ב-23:58 מיוחס נכון לאותו יום,
    גם אם ה-run קורה ב-00:40 של היום הבא.
    """
    daily = {}
    if not rain_sensor or 'values' not in rain_sensor:
        return daily

    rain_values = rain_sensor['values']
    key = 'sum' if 'sum' in rain_values and rain_values['sum'] else 'raw'
    if key not in rain_values or not rain_values[key]:
        return daily

    vals = rain_values[key]
    for i, date in enumerate(dates):
        if date is None or i >= len(vals):
            continue
        v = vals[i]
        if v is None:
            continue
        date_str = date.strftime('%Y-%m-%d')
        daily[date_str] = round(daily.get(date_str, 0) + v, 2)

    print(f"📊 גשם מ-raw לפי יום: { {k: v for k, v in daily.items() if v > 0} }")
    return daily

def update_seasonal_rain(raw_daily):
    """
    עדכון משקעים עונתיים — שיטת raw timestamps + max().

    הלוגיקה:
      • raw_daily = dict של {תאריך: סכום_גשם} מחושב מנתוני raw
      • לכל תאריך: daily_rain[date] = max(קיים, חדש_מ_raw)
      • season_total = legacy_season_rain + sum(daily_rain)

    היתרון הכפול:
      1. timestamps מה-raw — גשם ב-23:58 מיוחס נכון
      2. max() — ערך שכבר נשמר לא יכול לרדת
    """
    season_data = load_season_data()

    # בדיקת תחילת עונה חדשה
    now = israel_now()
    season_start_str = f"{now.year if now.month >= 10 else now.year - 1}-10-01"
    if season_data.get('season_start') != season_start_str:
        print(f"🌱 עונה חדשה: {season_start_str}")
        season_data = {
            "season_start": season_start_str,
            "legacy_season_rain": 0.0,
            "daily_rain": {}
        }

    daily_rain = season_data.get('daily_rain', {})

    # עדכון כל תאריך שנמצא ב-raw — רק מעלה, אף פעם לא מוריד
    for date_str, rain_val in raw_daily.items():
        # רק תאריכים בתוך העונה
        if date_str < season_start_str:
            continue
        existing = daily_rain.get(date_str, 0)
        if rain_val > existing:
            print(f"✅ עדכון {date_str}: {existing} → {rain_val} מ\"מ")
            daily_rain[date_str] = rain_val
        else:
            if existing > 0:
                print(f"   {date_str}: נשאר {existing} מ\"מ")

    season_data['daily_rain'] = daily_rain
    save_season_data(season_data)

    legacy = season_data.get('legacy_season_rain', 0)
    daily_sum = round(sum(daily_rain.values()), 1)
    total = round(legacy + daily_sum, 1)
    print(f"☔ עונתי: legacy={legacy} + daily={daily_sum} = {total} מ\"מ")
    return total

def extract_weather_data():
    """שליפה ועיבוד נתוני מזג אויר"""
    print("🌤️  שולף נתונים מ-FieldClimate...")

    station_info = make_request(f"/station/{STATION_ID}")
    if not station_info:
        print("❌ שגיאה בשליפת מידע התחנה")
        return None

    raw_24h = make_request(f"/data/{STATION_ID}/raw/last/24h")
    if not raw_24h:
        print("❌ שגיאה בשליפת נתונים")
        return None

    meta = station_info.get('meta', {})
    data = raw_24h.get('data', [])
    dates_raw = raw_24h.get('dates', [])

    # המרת dates — ה-API מחזיר זמן מקומי ישראל ישירות
    dates = []
    for date_str in dates_raw:
        try:
            dates.append(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S"))
        except:
            dates.append(None)

    # מציאת חיישנים
    temp_sensor = wind_speed_sensor = wind_dir_sensor = rain_sensor = None
    for sensor in data:
        name = sensor.get('name_original', '')
        if 'Air temperature' in name:
            temp_sensor = sensor
        elif 'wind speed' in name.lower():
            wind_speed_sensor = sensor
        elif 'wind dir' in name.lower():
            wind_dir_sensor = sensor
        elif 'Precipitation' in name:
            rain_sensor = sensor

    # חלון 24 שעות אחרונות
    now_israel = israel_now().replace(tzinfo=None)
    cutoff = now_israel - timedelta(hours=24)
    last_24h_indices = [i for i, d in enumerate(dates) if d and d >= cutoff]
    print(f"📊 24 שעות אחרונות: {len(last_24h_indices)} דגימות מ-raw")

    # טמפרטורה
    current_temp = temp_max = temp_max_time = temp_min = temp_min_time = None
    if temp_sensor and 'values' in temp_sensor:
        values = temp_sensor['values']
        if 'avg' in values and values['avg']:
            current_temp = values['avg'][-1]
        if last_24h_indices and 'max' in values and values['max']:
            vals = [values['max'][i] for i in last_24h_indices]
            temp_max = max(vals)
            temp_max_time = dates[last_24h_indices[vals.index(temp_max)]]
        if last_24h_indices and 'min' in values and values['min']:
            vals = [values['min'][i] for i in last_24h_indices]
            temp_min = min(vals)
            temp_min_time = dates[last_24h_indices[vals.index(temp_min)]]
    if temp_min is None:
        temp_min = meta.get('airTemperatureDailyMinimum')

    # רוח
    current_wind_speed = wind_max = wind_max_time = wind_direction = None
    if wind_speed_sensor and 'values' in wind_speed_sensor:
        values = wind_speed_sensor['values']
        if 'avg' in values and values['avg']:
            current_wind_speed = round(values['avg'][-1] * 3.6, 1)
        if last_24h_indices and 'max' in values and values['max']:
            vals = [values['max'][i] * 3.6 for i in last_24h_indices]
            wind_max = round(max(vals), 1)
            wind_max_time = dates[last_24h_indices[vals.index(max(vals))]]
    if wind_dir_sensor and 'values' in wind_dir_sensor:
        values = wind_dir_sensor['values']
        if 'last' in values and values['last']:
            wind_direction = degrees_to_direction(values['last'][-1])

    # גשם היום (לתצוגה בלבד — מ-meta כרגיל)
    rain_today_display = meta.get('rainCurrentDay', {}).get('sum', 0)
    rain_7d = meta.get('rain7d', {}).get('sum', 0)

    # גשם בשעה האחרונה
    rain_last_hour = 0
    if rain_sensor and 'values' in rain_sensor:
        rain_values = rain_sensor['values']
        key = 'sum' if 'sum' in rain_values and rain_values['sum'] else 'raw'
        if key in rain_values and rain_values[key]:
            rain_last_hour = sum(rain_values[key][-4:]) if len(rain_values[key]) >= 4 else 0

    # חישוב גשם יומי מ-raw timestamps → עדכון צבירה עונתית
    raw_daily = calc_rain_from_raw(dates, rain_sensor)
    season_total = update_seasonal_rain(raw_daily)

    # פאנל סולארי
    solar_raw = meta.get('solarPanel', 0)
    solar_panel = solar_raw if isinstance(solar_raw, (int, float)) else solar_raw.get('last', 0)

    print(f"🌧️  גשם בשעה האחרונה: {rain_last_hour} מ\"מ")
    print(f"☀️  פאנל סולארי: {solar_panel} mV")

    rain_7d_daily = get_7day_rain(meta)

    weather_data = {
        'last_update': datetime.utcnow().isoformat() + 'Z',
        'station_name': station_info.get('name', {}).get('custom', 'כוכב השחר'),
        'solarPanel': solar_panel,
        'temperature': {
            'current': round(current_temp, 1) if current_temp else None,
            'max': round(temp_max, 1) if temp_max else None,
            'max_time': format_time(temp_max_time) if temp_max_time else None,
            'min': round(temp_min, 1) if temp_min else None,
            'min_time': format_time(temp_min_time) if temp_min_time else None
        },
        'wind': {
            'speed': current_wind_speed,
            'direction': wind_direction,
            'max': wind_max,
            'max_time': format_time(wind_max_time) if wind_max_time else None
        },
        'rain': {
            'today': round(rain_today_display, 1),
            'lastHour': round(rain_last_hour, 1),
            'week': round(rain_7d, 1),
            'season': round(season_total + PRE_STATION_RAIN, 1),
            'daily_7d': rain_7d_daily
        }
    }

    return weather_data

def degrees_to_direction(deg):
    directions = ['צפון', 'צ-מז', 'מזרח', 'ד-מז', 'דרום', 'ד-מע', 'מערב', 'צ-מע']
    return directions[int((deg + 22.5) / 45) % 8]

def format_time(time_input):
    try:
        if isinstance(time_input, datetime):
            return time_input.strftime('%H:%M')
        return datetime.fromisoformat(time_input.replace('Z', '+00:00')).strftime('%H:%M')
    except:
        return str(time_input) if time_input else None

def get_7day_rain(meta):
    rain_7d = meta.get('rain7d', {})
    vals = rain_7d.get('vals', [0, 0, 0, 0, 0, 0, 0])
    while len(vals) < 7:
        vals.insert(0, 0)
    return [round(v, 1) for v in vals[-7:]]

def main():
    print("=" * 50)
    print("Weather Dashboard - Data Fetcher")
    print("=" * 50)
    print(f"🕐 שעון ישראל: {israel_now().strftime('%Y-%m-%d %H:%M:%S %Z')}")

    weather_data = extract_weather_data()

    if not weather_data:
        print("\n❌ כשל בשליפת הנתונים")
        return

    with open('weather-data.json', 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ הנתונים נשמרו בהצלחה!")
    print(f"📁 קובץ: weather-data.json")
    print(f"\n📊 סיכום:")
    print(f"   🌡️  טמפרטורה: {weather_data['temperature']['current']}°C")
    print(f"   📈 מקסימום: {weather_data['temperature']['max']}°C ({weather_data['temperature']['max_time']})")
    print(f"   📉 מינימום: {weather_data['temperature']['min']}°C ({weather_data['temperature']['min_time']})")
    print(f"   💨 רוח: {weather_data['wind']['speed']} קמ\"ש {weather_data['wind']['direction']}")
    print(f"   💨🔝 רוח מקסימלית (24 שעות): {weather_data['wind']['max']} קמ\"ש ({weather_data['wind']['max_time']})")
    print(f"   ☀️  פאנל סולארי: {weather_data['solarPanel']} mV")
    print(f"   🌧️  גשם היום: {weather_data['rain']['today']} מ\"מ")
    print(f"   📅 גשם שבועי: {weather_data['rain']['week']} מ\"מ")
    print(f"   ☔ גשם עונתי: {weather_data['rain']['season']} מ\"מ (כולל {PRE_STATION_RAIN} מ\"מ טרום-תחנה)")

if __name__ == "__main__":
    main()
