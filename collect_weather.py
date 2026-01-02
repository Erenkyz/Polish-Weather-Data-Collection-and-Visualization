"""
Polish Weather Data Collector
Manual collection script - Run 3 times per day (morning, afternoon, evening)
Cities: Warsaw, Krakow, Wroclaw, Gdansk
"""

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import time

# ==============================
# CONFIGURATION
# ==============================

API_KEY = "f5d78cc13d1ee7bc357815a64011285a"

CITIES = [
    ("Warsaw", "PL"),
    ("Krakow", "PL"),
    ("Wroclaw", "PL"),
    ("Gdansk", "PL")
]

DB_CONFIG = {
    'dbname': 'weather_db',
    'user': 'postgres',
    'password': 'password',  # Güvenlik için .env önerilir ama şimdilik böyle
    'host': 'localhost',
    'port': '5432'
}

# ==============================
# FUNCTIONS
# ==============================

def fetch_weather_data(city, country):
    """Fetch current weather data from OpenWeatherMap API"""
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': f'{city},{country}',
        'appid': API_KEY,
        'units': 'metric',
        'lang': 'en'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   API Error {response.status_code} for {city}")
            return None
    except Exception as e:
        print(f"   Request failed for {city}: {e}")
        return None


def transform_weather_data(raw_data):
    """Transform raw API response into clean dictionary"""
    if not raw_data or raw_data.get('cod') not in [200, "200"]:
        return None
    
    try:
        return {
            'city': raw_data['name'],
            'country': raw_data['sys']['country'],
            'latitude': raw_data['coord']['lat'],
            'longitude': raw_data['coord']['lon'],
            'temperature': round(raw_data['main']['temp'], 2),
            'feels_like': round(raw_data['main']['feels_like'], 2),
            'temp_min': round(raw_data['main']['temp_min'], 2),
            'temp_max': round(raw_data['main']['temp_max'], 2),
            'pressure': raw_data['main']['pressure'],
            'humidity': raw_data['main']['humidity'],
            'weather_main': raw_data['weather'][0]['main'],
            'weather_description': raw_data['weather'][0]['description'],
            'wind_speed': round(raw_data['wind']['speed'], 2),
            'wind_direction': raw_data['wind'].get('deg', 0),
            'clouds': raw_data['clouds']['all'],
            'visibility': raw_data.get('visibility', 10000),
            'sunrise': datetime.fromtimestamp(raw_data['sys']['sunrise']),
            'sunset': datetime.fromtimestamp(raw_data['sys']['sunset']),
            'recorded_at': datetime.fromtimestamp(raw_data['dt'])
        }
    except Exception as e:
        print(f"   Transformation failed: {e}")
        return None


def save_to_database(conn, data):
    """Insert transformed data into PostgreSQL"""
    sql = """
    INSERT INTO weather_data (
        city, country, latitude, longitude,
        temperature, feels_like, temp_min, temp_max,
        pressure, humidity,
        weather_main, weather_description,
        wind_speed, wind_direction,
        clouds, visibility,
        sunrise, sunset, recorded_at
    ) VALUES (
        %(city)s, %(country)s, %(latitude)s, %(longitude)s,
        %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s,
        %(pressure)s, %(humidity)s,
        %(weather_main)s, %(weather_description)s,
        %(wind_speed)s, %(wind_direction)s,
        %(clouds)s, %(visibility)s,
        %(sunrise)s, %(sunset)s, %(recorded_at)s
    )
    ON CONFLICT (city, recorded_at) DO NOTHING
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql, data)
        if cursor.rowcount > 0:
            conn.commit()
            return 'success'
        else:
            return 'duplicate'
    except Exception as e:
        conn.rollback()
        print(f"   DB Error: {e}")
        return 'error'
    finally:
        cursor.close()


def collect_now():
    """Main collection routine"""
    print("\n" + "="*70)
    print(" MANUAL WEATHER DATA COLLECTION")
    print("="*70)
    print(f" Date: {datetime.now().strftime('%Y-%m-%d')}")
    print(f" Time: {datetime.now().strftime('%H:%M:%S')}")
    print("="*70)

    # Connect to DB
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(" Connected to database")
    except Exception as e:
        print(f" Database connection failed: {e}")
        return

    print("\n Collecting from 4 cities...\n")
    stats = {'success': 0, 'duplicate': 0, 'failed': 0}

    for i, (city, country) in enumerate(CITIES, 1):
        print(f"[{i}/4] {city}, {country}")

        # Fetch
        print("   Fetching...", end=" ")
        raw_data = fetch_weather_data(city, country)
        if not raw_data:
            print("Failed")
            stats['failed'] += 1
            continue
        print("OK")

        # Transform
        print("   Transforming...", end=" ")
        clean_data = transform_weather_data(raw_data)
        if not clean_data:
            print("Failed")
            stats['failed'] += 1
            continue
        print("OK")

        # Save
        print("   Saving...", end=" ")
        result = save_to_database(conn, clean_data)
        if result == 'success':
            print("Saved!")
            print(f"      {clean_data['temperature']}°C, {clean_data['weather_description']}")
            stats['success'] += 1
        elif result == 'duplicate':
            print("Duplicate")
            stats['duplicate'] += 1
        else:
            print("Failed")
            stats['failed'] += 1

        time.sleep(1)

    conn.close()

    # Session summary
    print("\n" + "="*70)
    print(" THIS SESSION SUMMARY")
    print("="*70)
    print(f" New records: {stats['success']}")
    print(f" Duplicates : {stats['duplicate']}")
    print(f" Failed     : {stats['failed']}")

    # Total progress
    print("\n" + "="*70)
    print(" TOTAL PROGRESS")
    print("="*70)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM weather_data")
        total = cur.fetchone()[0]

        cur.execute("SELECT city, COUNT(*) FROM weather_data GROUP BY city ORDER BY city")
        city_counts = cur.fetchall()

        cur.execute("SELECT MIN(DATE(recorded_at)), MAX(DATE(recorded_at)) FROM weather_data")
        first_day, last_day = cur.fetchone()

        cur.close()
        conn.close()

        target = 108
        percentage = (total / target) * 100

        print(f"Total records: {total} / {target} ({percentage:.1f}%)")
        print("\nRecords per city:")
        for city, count in city_counts:
            print(f"  {city:15} : {count} records")

        if first_day and last_day:
            days = (last_day - first_day).days + 1
            print(f"\nCollection period: {days} day(s)")
            print(f"First: {first_day}")
            print(f"Last : {last_day}")

        # Progress bar
        bar_length = 50
        filled = int(percentage / 100 * bar_length)
        bar = "█" * filled + "░" * (bar_length - filled)
        print(f"\nProgress: [{bar}] {percentage:.1f}%")

        if total >= target:
            print("\n TARGET REACHED! You have 100+ records!")
        else:
            remaining = target - total
            next_runs = (remaining + 3) // 4
            print(f"\n Need {next_runs} more runs to reach target")
            print(f" (About {(next_runs + 2) // 3} more days at 3 runs/day)")

    except Exception as e:
        print(f"Could not fetch progress: {e}")

    print("="*70)
    print("\n Collection complete!")
    print("="*70)


def show_schedule_reminder():
    """Suggest next run time"""
    now = datetime.now()
    times = [(8, 0, "Morning"), (14, 0, "Afternoon"), (20, 0, "Evening")]
    
    next_time = None
    for hour, minute, name in times:
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate > now:
            next_time = (name, candidate)
            break
    
    if not next_time:
        tomorrow = now + timedelta(days=1)
        next_time = ("Morning", tomorrow.replace(hour=8, minute=0, second=0, microsecond=0))
    
    name, target = next_time
    diff = target - now
    hours = int(diff.total_seconds() // 3600)
    minutes = int((diff.total_seconds() % 3600) // 60)
    
    print(f"\n Next suggested run: {name} at {target.strftime('%H:%M')} "
          f"(in {hours}h {minutes}m)")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    collect_now()
    show_schedule_reminder()