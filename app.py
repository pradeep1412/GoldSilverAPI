from flask import Flask, jsonify
import requests
from bs4 import BeautifulSoup
import logging

# -------------------- App Setup --------------------
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------- Scraping --------------------
def get_goodreturns_prices():
    """Scrape gold, silver, platinum, nifty prices directly from GoodReturns"""
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

        return result

    except Exception as e:
        logging.error(f"Scraping error: {str(e)}")
        return {"error": str(e)}


# -------------------- Endpoints --------------------
@app.route('/api/gold', methods=['GET'])
def gold():
    data = get_goodreturns_prices()
    if "error" in data:
        return jsonify(data), 500
    return jsonify(data["gold"])

@app.route('/api/silver', methods=['GET'])
def silver():
    data = get_goodreturns_prices()
    if "error" in data:
        return jsonify(data), 500
    return jsonify(data["silver"])

@app.route('/api/platinum', methods=['GET'])
def platinum():
    data = get_goodreturns_prices()
    if "error" in data:
        return jsonify(data), 500
    return jsonify(data["platinum"])

@app.route('/api/nifty', methods=['GET'])
def nifty():
    data = get_goodreturns_prices()
    if "error" in data:
        return jsonify(data), 500
    return jsonify({"nifty": data["nifty"]})

@app.route('/api/all', methods=['GET'])
def all_prices():
    data = get_goodreturns_prices()
    return jsonify(data)

@app.route('/')
def home():
    return "Hello World! Metal Prices API is running 🚀"


# -------------------- Run App --------------------
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
