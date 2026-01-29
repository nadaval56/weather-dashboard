#!/usr/bin/env python3
"""
Weather Dashboard Data Fetcher
מושך נתונים מ-FieldClimate ומעבד אותם לדשבורד
"""

import requests
import hmac
import hashlib
from datetime import datetime, timedelta
import json
import os

# ===== הגדרות =====
# קריאה ממשתני סביבה (GitHub Secrets)
STATION_ID = os.environ.get('STATION_ID', '03114DE5')
PUBLIC_KEY = os.environ.get('PUBLIC_KEY')
PRIVATE_KEY = os.environ.get('PRIVATE_KEY')

# בדיקה שהמפתחות קיימים (רק אזהרה, לא עוצרים)
if not PUBLIC_KEY or not PRIVATE_KEY:
    print("⚠️  אזהרה: לא נמצאו מפתחות API ב-environment variables")

API_BASE = "https://api.fieldclimate.com/v2"

# גשם שנרשם לפני הקמת התחנה (ניתן לעדכן כשיהיו נתונים מדויקים יותר)
PRE_STATION_RAIN = 15.0  # מ"מ - מדידות מתחנות אחרות לפני הקמת התחנה

def make_request(path):
    """
    שליחת בקשה ל-API עם אימות HMAC
    """
    url = f"{API_BASE}{path}"
    timestamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    
    # חתימת HMAC
    content_to_sign = f"GET{path}{timestamp}{PUBLIC_KEY}"
    signature = hmac.new(
        PRIVATE_KEY.encode('utf-8'),
        content_to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    # Headers
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

def extract_weather_data():
    """
    שליפה ועיבוד נתוני מזג אויר
    """
    print("🌤️  שולף נתונים מ-FieldClimate...")
    
    # שליפת מידע על התחנה
    station_info = make_request(f"/station/{STATION_ID}")
    if not station_info:
        print("❌ שגיאה בשליפת מידע התחנה")
        return None
    
    # שליפת נתונים (24 שעות אחרונות + 7 ימים)
    raw_24h = make_request(f"/data/{STATION_ID}/raw/last/24h")
    if not raw_24h:
        print("❌ שגיאה בשליפת נתונים")
        return None
    
    # חילוץ מטא-דאטה (משקעים לפי תקופות)
    meta = station_info.get('meta', {})
    
    # חילוץ נתוני חיישנים
    data = raw_24h.get('data', [])
    dates_raw = raw_24h.get('dates', [])
    
    # המרת dates ל-datetime objects
    dates = []
    for date_str in dates_raw:
        try:
            # פורמט: "2026-01-28 10:00:00" (בזמן מקומי UTC+2!)
            date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            # ה-dates כבר בזמן מקומי - לא צריך להמיר!
            dates.append(date_obj)
        except Exception as e:
            dates.append(None)
    
    # מציאת החיישנים
    temp_sensor = None
    wind_speed_sensor = None
    wind_dir_sensor = None
    rain_sensor = None
    
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
    
    # חילוץ ערכים
    current_temp = None
    temp_max = None
    temp_max_time = None
    temp_min = None
    temp_min_time = None
    
    if temp_sensor and 'values' in temp_sensor:
        values = temp_sensor['values']
        if 'avg' in values and values['avg']:
            current_temp = values['avg'][-1]  # ערך אחרון
        
        # ה-API מחזיר 24 שעות - פשוט נקח את המקסימום והמינימום
        if 'max' in values and values['max'] and dates:
            temp_max_list = values['max']
            temp_max = max(temp_max_list)
            temp_max_idx = temp_max_list.index(temp_max)
            temp_max_time = dates[temp_max_idx]
        
        if 'min' in values and values['min'] and dates:
            temp_min_list = values['min']
            temp_min = min(temp_min_list)
            temp_min_idx = temp_min_list.index(temp_min)
            temp_min_time = dates[temp_min_idx]
    
    # רוח
    current_wind_speed = None
    wind_max = None
    wind_max_time = None
    wind_direction = None
    
    if wind_speed_sensor and 'values' in wind_speed_sensor:
        values = wind_speed_sensor['values']
        if 'avg' in values and values['avg']:
            # המרה מ-m/s ל-km/h
            current_wind_speed = round(values['avg'][-1] * 3.6, 1)
        
        # ה-API מחזיר 24 שעות - פשוט נקח את המקסימום
        if 'max' in values and values['max'] and dates:
            wind_max_list = [v * 3.6 for v in values['max']]  # המרה ל-km/h
            wind_max = round(max(wind_max_list), 1)
            wind_max_idx = wind_max_list.index(max(wind_max_list))
            wind_max_time = dates[wind_max_idx]
    
    if wind_dir_sensor and 'values' in wind_dir_sensor:
        values = wind_dir_sensor['values']
        if 'last' in values and values['last']:
            wind_deg = values['last'][-1]
            # המרה לכיוון טקסט
            wind_direction = degrees_to_direction(wind_deg)
    
    # משקעים
    rain_today = meta.get('rainCurrentDay', {}).get('sum', 0)
    rain_7d = meta.get('rain7d', {}).get('sum', 0)
    
    # חישוב גשם בשעה האחרונה (4 דגימות אחרונות × 15 דקות)
    rain_last_hour = 0
    if rain_sensor and 'values' in rain_sensor:
        rain_values = rain_sensor['values']
        # נסה למצוא את הערכים - יכול להיות 'sum' או 'raw'
        if 'sum' in rain_values and rain_values['sum']:
            # קח את 4 הערכים האחרונים (שעה אחרונה)
            rain_last_hour = sum(rain_values['sum'][-4:]) if len(rain_values['sum']) >= 4 else 0
        elif 'raw' in rain_values and rain_values['raw']:
            rain_last_hour = sum(rain_values['raw'][-4:]) if len(rain_values['raw']) >= 4 else 0
    
    # עדכון וחישוב משקעים עונתיים (שיטת צבירה)
    rain_season = update_seasonal_rain(rain_today)
    
    print(f"🌧️  גשם עונתי מחושב: {rain_season} מ\"מ")
    print(f"🌧️  גשם בשעה האחרונה: {rain_last_hour} מ\"מ")
    
    # חישוב גשם ל-7 ימים אחרונים (לגרף)
    rain_7d_daily = get_7day_rain(meta)
    
    # הכנת המידע המעובד
    weather_data = {
        'last_update': datetime.utcnow().isoformat() + 'Z',
        'station_name': station_info.get('name', {}).get('custom', 'כוכב השחר'),
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
            'lastHour': round(rain_last_hour, 1),  # גשם בשעה האחרונה
            'week': round(rain_7d, 1),
            'season': round(rain_season + PRE_STATION_RAIN, 1),  # כולל גשם טרום-תחנה
            'daily_7d': rain_7d_daily
        }
    }
    
    return weather_data

def degrees_to_direction(deg):
    """
    המרת מעלות לכיוון רוח
    """
    directions = [
        'צפון', 'צ-מז', 'מזרח', 'ד-מז',
        'דרום', 'ד-מע', 'מערב', 'צ-מע'
    ]
    idx = int((deg + 22.5) / 45) % 8
    return directions[idx]

def format_time(time_input):
    """
    עיצוב זמן לפורמט נוח
    """
    try:
        # אם זה כבר datetime object
        if isinstance(time_input, datetime):
            dt = time_input
        else:
            # אם זה string
            dt = datetime.fromisoformat(time_input.replace('Z', '+00:00'))
        
        # המרה לזמן ישראל (+2 שעות)
        dt = dt + timedelta(hours=2)
        return dt.strftime('%H:%M')
    except:
        return str(time_input) if time_input else None

def calculate_season_rain(meta):
    """
    חישוב משקעים מתחילת העונה (1 באוקטובר)
    """
    # בינתיים - פשוט נחזיר את השבועי
    # TODO: נוסיף שליפה של נתונים יומיים
    return meta.get('rain7d', {}).get('sum', 0)

def load_season_data():
    """
    טעינת נתוני עונה שמורים
    """
    try:
        with open('rain_season.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        # אתחול ראשוני - ערך ידני מהאתר של FieldClimate
        print("📝 יצירת קובץ עונתי ראשוני...")
        return {
            "season_start": "2025-10-01",
            "season_rain": 223.2,  # ערך ידני מהאתר נכון ל-27/01/2026
            "last_update": "2026-01-27",
            "last_daily_rain": 0
        }

def save_season_data(season_data):
    """
    שמירת נתוני עונה
    """
    with open('rain_season.json', 'w', encoding='utf-8') as f:
        json.dump(season_data, f, ensure_ascii=False, indent=2)

def update_seasonal_rain(rain_today):
    """
    עדכון משקעים עונתיים - שיטת צבירה
    """
    print(f"📊 עדכון משקעים עונתיים - גשם היום: {rain_today} מ\"מ")
    
    # טעינת נתונים קיימים
    season_data = load_season_data()
    print(f"📂 נתונים קיימים: {season_data}")
    
    # בדיקה אם צריך לאפס (1 באוקטובר)
    now = datetime.utcnow()
    season_start_str = f"{now.year if now.month >= 10 else now.year - 1}-10-01"
    
    if season_data.get('season_start') != season_start_str:
        # עונה חדשה!
        print(f"🌱 עונה חדשה מתחילה: {season_start_str}")
        season_data = {
            "season_start": season_start_str,
            "season_rain": 0,
            "last_update": now.strftime('%Y-%m-%d'),
            "last_daily_rain": 0
        }
    
    # בדיקה אם כבר עדכנו היום
    today_str = now.strftime('%Y-%m-%d')
    if season_data.get('last_update') == today_str:
        # כבר עדכנו היום - לא להוסיף שוב
        print(f"✅ כבר עודכן היום - מחזיר: {season_data['season_rain']} מ\"מ")
        return season_data['season_rain']
    
    # עדכון הסכום
    old_rain = season_data.get('season_rain', 0)
    new_rain = old_rain + rain_today
    
    season_data['season_rain'] = new_rain
    season_data['last_update'] = today_str
    season_data['last_daily_rain'] = rain_today
    
    # שמירה
    save_season_data(season_data)
    
    print(f"☔ עדכון עונתי: {old_rain} + {rain_today} = {new_rain} מ\"מ")
    print(f"💾 נשמר בהצלחה!")
    
    return new_rain

def fetch_seasonal_rain():
    """
    שליפת נתונים יומיים לחישוב משקעים עונתיים
    """
    # פשוט נחזיר את הערך השמור
    season_data = load_season_data()
    return season_data.get('season_rain', 0)

def get_7day_rain(meta):
    """
    חילוץ משקעים יומיים ל-7 ימים אחרונים
    """
    rain_7d = meta.get('rain7d', {})
    vals = rain_7d.get('vals', [0, 0, 0, 0, 0, 0, 0])
    
    # ודא שיש 7 ערכים
    while len(vals) < 7:
        vals.insert(0, 0)
    
    # עיגול
    return [round(v, 1) for v in vals[-7:]]

def main():
    print("="*50)
    print("Weather Dashboard - Data Fetcher")
    print("="*50)
    
    # שליפת נתונים
    weather_data = extract_weather_data()
    
    if not weather_data:
        print("\n❌ כשל בשליפת הנתונים")
        return
    
    # שמירה לקובץ JSON
    output_file = 'weather-data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ הנתונים נשמרו בהצלחה!")
    print(f"📁 קובץ: {output_file}")
    print(f"\n📊 סיכום:")
    print(f"   🌡️  טמפרטורה: {weather_data['temperature']['current']}°C")
    print(f"   📈 מקסימום: {weather_data['temperature']['max']}°C ({weather_data['temperature']['max_time']})")
    print(f"   📉 מינימום: {weather_data['temperature']['min']}°C ({weather_data['temperature']['min_time']})")
    print(f"   💨 רוח: {weather_data['wind']['speed']} קמ\"ש {weather_data['wind']['direction']}")
    print(f"   💨🔝 רוח מקסימלית (24 שעות): {weather_data['wind']['max']} קמ\"ש ({weather_data['wind']['max_time']})")
    print(f"   🌧️  גשם היום: {weather_data['rain']['today']} מ\"מ")
    print(f"   📅 גשם שבועי: {weather_data['rain']['week']} מ\"מ")
    print(f"   ☔ גשם עונתי: {weather_data['rain']['season']} מ\"מ (כולל {PRE_STATION_RAIN} מ\"מ טרום-תחנה)")

if __name__ == "__main__":
    main()
