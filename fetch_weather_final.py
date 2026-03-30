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

# שעון ישראל — תמיד נכון, גם בשעון קיץ וגם בשעון חורף
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

def israel_now():
    """מחזיר את השעה הנוכחית בישראל — כולל שעון קיץ/חורף אוטומטית"""
    return datetime.now(ISRAEL_TZ)

def israel_today():
    """מחזיר את תאריך היום בישראל כ-string"""
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
    """טעינת נתוני עונה שמורים"""
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
    """שמירת נתוני עונה"""
    with open('rain_season.json', 'w', encoding='utf-8') as f:
        json.dump(season_data, f, ensure_ascii=False, indent=2)

def update_seasonal_rain(rain_today):
    """
    עדכון משקעים עונתיים — שיטת מקסימום יומי.

    הלוגיקה:
      • שומרים dict של גשם לפי תאריך: daily_rain["2026-03-29"] = 4.6
      • בכל run: daily_rain[היום] = max(ערך קיים, rain_today)
      • season_total = legacy_season_rain + sum(daily_rain.values())

    היתרון: הערך היומי רק עולה, אף פעם לא יורד.
    לא משנה אם FieldClimate איפס, אם GitHub Actions איחר,
    או אם עברנו שעון — הנתון כבר נעול.
    """
    today = israel_today()
    print(f"📊 עדכון גשם — היום: {today}, rain_today: {rain_today} מ\"מ")

    season_data = load_season_data()

    # בדיקה אם צריך לאפס (תחילת עונה חדשה — 1 באוקטובר)
    now = israel_now()
    season_start_str = f"{now.year if now.month >= 10 else now.year - 1}-10-01"
    if season_data.get('season_start') != season_start_str:
        print(f"🌱 עונה חדשה מתחילה: {season_start_str}")
        season_data = {
            "season_start": season_start_str,
            "legacy_season_rain": 0.0,
            "daily_rain": {}
        }

    daily_rain = season_data.get('daily_rain', {})

    # עדכון — רק אם הערך החדש גדול מהקיים
    existing = daily_rain.get(today, 0)
    if rain_today > existing:
        daily_rain[today] = rain_today
        print(f"✅ עדכון {today}: {existing} → {rain_today} מ\"מ")
    else:
        daily_rain[today] = existing
        print(f"✅ {today}: נשאר {existing} מ\"מ (rain_today={rain_today} לא גדול יותר)")

    season_data['daily_rain'] = daily_rain
    save_season_data(season_data)

    legacy = season_data.get('legacy_season_rain', 0)
    total = legacy + sum(daily_rain.values())
    print(f"☔ עונתי: legacy={legacy} + daily={round(sum(daily_rain.values()),1)} = {round(total,1)} מ\"מ")
    return round(total, 1)

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

    # משקעים
    rain_today = meta.get('rainCurrentDay', {}).get('sum', 0)
    rain_7d = meta.get('rain7d', {}).get('sum', 0)

    # גשם בשעה האחרונה
    rain_last_hour = 0
    if rain_sensor and 'values' in rain_sensor:
        rain_values = rain_sensor['values']
        key = 'sum' if 'sum' in rain_values and rain_values['sum'] else 'raw'
        if key in rain_values and rain_values[key]:
            rain_last_hour = sum(rain_values[key][-4:]) if len(rain_values[key]) >= 4 else 0

    # עדכון צבירה עונתית
    season_total = update_seasonal_rain(rain_today)

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
            'today': round(rain_today, 1),
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
