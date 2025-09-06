from flask import Flask, jsonify, request
import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime
from functools import lru_cache
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import time
import logging
from datetime import datetime
from contextlib import closing

# -------------------- App Setup --------------------
app = Flask(__name__)
CACHE_TIMEOUT = 300  # Cache prices for 5 minutes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------- DB Init --------------------
def init_db():
    with closing(sqlite3.connect('metal_prices.db')) as conn:
        cursor = conn.cursor()

        # ---------------- Gold ----------------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                price_24k REAL,
                price_22k REAL,
                price_18k REAL,
                unit TEXT,
                city TEXT,
                source TEXT,
                timestamp INTEGER
            )
        ''')

        # ---------------- Silver ----------------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS silver_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                price REAL,
                unit TEXT,
                city TEXT,
                source TEXT,
                timestamp INTEGER
            )
        ''')

        # ---------------- Platinum ----------------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS platinum_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                price REAL,
                unit TEXT,
                city TEXT,
                source TEXT,
                timestamp INTEGER
            )
        ''')

        # ---------------- Nifty ----------------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nifty_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                price REAL,
                source TEXT,
                timestamp INTEGER
            )
        ''')

        conn.commit()


init_db()

scheduler = BackgroundScheduler()

def scheduled_price_fetch():
    logging.info("Scheduled job: Fetching metal prices...")
    prices = get_goodreturns_prices()
    if prices:
        store_prices_in_db(prices)
        logging.info("Scheduled job: Prices saved to DB")
    else:
        logging.warning("Scheduled job: Failed to fetch prices")

# Run every 1 hour
scheduler.add_job(func=scheduled_price_fetch, trigger="interval", hours=1)

# Start scheduler
scheduler.start()

# Shut down scheduler when exiting
atexit.register(lambda: scheduler.shutdown())


# -------------------- Scraping --------------------
def get_goodreturns_prices():
    """Scrape gold, silver, platinum, nifty prices and save in DB"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}

        # ---------------- Gold ----------------
        gold_url = "https://www.goodreturns.in/gold-rates/"
        gold_response = requests.get(gold_url, headers=headers, timeout=10)
        gold_soup = BeautifulSoup(gold_response.text, 'html.parser')

        gold_prices = {
            "24k": gold_soup.find("span", {"id": "24K-price"}).get_text(),
            "22k": gold_soup.find("span", {"id": "22K-price"}).get_text(),
            "18k": gold_soup.find("span", {"id": "18K-price"}).get_text(),
        }

        nifty_price = None
        for div in gold_soup.find_all("div", {"class": "marquee-item"}):
            if "nifty" in div.get_text().lower():
                nifty_price = div.find("span", {"class": "stock-price"}).get_text()

        # ---------------- Silver ----------------
        silver_url = "https://www.goodreturns.in/silver-rates/"
        silver_response = requests.get(silver_url, headers=headers, timeout=10)
        silver_soup = BeautifulSoup(silver_response.text, 'html.parser')
        silver_price = silver_soup.find("span", {"id": "silver-1g-price"}).get_text()

        # ---------------- Platinum ----------------
        platinum_url = "https://www.goodreturns.in/platinum-price.html"
        platinum_response = requests.get(platinum_url, headers=headers, timeout=10)
        platinum_soup = BeautifulSoup(platinum_response.text, 'html.parser')
        platinum_price = platinum_soup.find("span", {"id": "platinum-1g-price"}).get_text()

        # ---------------- Result ----------------
        result = {
            "gold": {"unit": "1 gram", **gold_prices},
            "silver": {"unit": "1 gram", "price": silver_price},
            "platinum": {"unit": "1 gram", "price": platinum_price},
            "nifty": nifty_price,
            "currency": "INR",
            "source": "GoodReturns",
        }
        logging.info("fetch")

        # ✅ Store into DB
        store_prices_in_db(result)

        return result

    except Exception as e:
        logging.error(f"Scraping error: {str(e)}")
        return None


# -------------------- Database Store --------------------
def store_prices_in_db(prices):
    try:
        with closing(sqlite3.connect('metal_prices.db')) as conn:
            cursor = conn.cursor()
            current_time = int(time.time())
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Gold
            if 'gold' in prices:
                cursor.execute('''
                    INSERT INTO gold_prices (date, price_24k, price_22k, price_18k, unit, city, source, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    current_date,
                    prices['gold'].get('24k'),
                    prices['gold'].get('22k'),
                    prices['gold'].get('18k'),
                    prices['gold']['unit'],
                    'National Average',
                    prices['source'],
                    current_time
                ))

            # Silver
            if 'silver' in prices:
                cursor.execute('''
                    INSERT INTO silver_prices (date, price, unit, city, source, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    current_date,
                    prices['silver']['price'],
                    prices['silver']['unit'],
                    'National Average',
                    prices['source'],
                    current_time
                ))

            # Platinum
            if 'platinum' in prices:
                cursor.execute('''
                    INSERT INTO platinum_prices (date, price, unit, city, source, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    current_date,
                    prices['platinum']['price'],
                    prices['platinum']['unit'],
                    'National Average',
                    prices['source'],
                    current_time
                ))

            # Nifty
            if 'nifty' in prices:
                cursor.execute('''
                    INSERT INTO nifty_prices (date, price, source, timestamp)
                    VALUES (?, ?, ?, ?)
                ''', (
                    current_date,
                    prices['nifty'],
                    prices['source'],
                    current_time
                ))

            conn.commit()
            logging.info("Prices stored in DB")

    except Exception as e:
        logging.error(f"DB insert error: {str(e)}")


# -------------------- DB Endpoints --------------------
from flask import request

@app.route('/api/history/gold', methods=['GET'])
def gold_history():
    try:
        date_filter = request.args.get("date")  # optional query param
        with closing(sqlite3.connect('metal_prices.db')) as conn:
            cursor = conn.cursor()
            if date_filter:
                cursor.execute("""
                    SELECT date, price_24k, price_22k, price_18k, unit, city, source
                    FROM gold_prices
                    WHERE date = ?
                    ORDER BY id DESC
                """, (date_filter,))
            else:
                cursor.execute("""
                    SELECT date, price_24k, price_22k, price_18k, unit, city, source
                    FROM gold_prices
                    ORDER BY id DESC LIMIT 50
                """)
            rows = cursor.fetchall()
        return jsonify([{
            "date": r[0], "24k": r[1], "22k": r[2], "18k": r[3],
            "unit": r[4], "city": r[5]
        } for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/silver', methods=['GET'])
def silver_history():
    try:
        date_filter = request.args.get("date")
        with closing(sqlite3.connect('metal_prices.db')) as conn:
            cursor = conn.cursor()
            if date_filter:
                cursor.execute("""
                    SELECT date, price, unit, city, source
                    FROM silver_prices
                    WHERE date = ?
                    ORDER BY id DESC
                """, (date_filter,))
            else:
                cursor.execute("""
                    SELECT date, price, unit, city, source
                    FROM silver_prices
                    ORDER BY id DESC LIMIT 50
                """)
            rows = cursor.fetchall()
        return jsonify([{
            "date": r[0], "price": r[1], "unit": r[2], "city": r[3]
        } for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/platinum', methods=['GET'])
def platinum_history():
    try:
        date_filter = request.args.get("date")
        with closing(sqlite3.connect('metal_prices.db')) as conn:
            cursor = conn.cursor()
            if date_filter:
                cursor.execute("""
                    SELECT date, price, unit, city, source
                    FROM platinum_prices
                    WHERE date = ?
                    ORDER BY id DESC
                """, (date_filter,))
            else:
                cursor.execute("""
                    SELECT date, price, unit, city, source
                    FROM platinum_prices
                    ORDER BY id DESC LIMIT 50
                """)
            rows = cursor.fetchall()
        return jsonify([{
            "date": r[0], "price": r[1], "unit": r[2], "city": r[3]
        } for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/nifty', methods=['GET'])
def nifty_history():
    try:
        date_filter = request.args.get("date")
        with closing(sqlite3.connect('metal_prices.db')) as conn:
            cursor = conn.cursor()
            if date_filter:
                cursor.execute("""
                    SELECT date, price, source
                    FROM nifty_prices
                    WHERE date = ?
                    ORDER BY id DESC
                """, (date_filter,))
            else:
                cursor.execute("""
                    SELECT date, price, source
                    FROM nifty_prices
                    ORDER BY id DESC LIMIT 50
                """)
            rows = cursor.fetchall()
        return jsonify([{
            "date": r[0], "price": r[1]
        } for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def home():
    return "hello world!"

# -------------------- Run App --------------------
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)