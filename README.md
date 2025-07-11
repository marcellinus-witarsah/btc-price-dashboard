# BTC Price Dashboard
<p align="center">
    <img src="assets/2025-06-01 21-37-04.mp4.gif" alt="Candlestick Chart Image" height="500">
    <p align="center">
        Figure 1: Candlestick Chart Dashboard (GIF by Author).
    </p>
</p>

## Project Summary
This project shows a real-time candlestick chart of Bitcoin prices using data from the Kraken WebSocket API.

## Project Scope
The goal is to create a candlestick chart that supports 10-second, 30-second, and 1-minute timeframes to show real-time data shown on a dashboard.

## Project Documentation Link
You can visit about the project explanation in this <a href="https://medium.com/@aw_marcell/real-time-data-streaming-project-bitcoin-live-price-dashboard-dda614c28177">Medium blog</a>.

## Technologies and Libraries

### Technologies
- **Python** – Core programming language used for data processing and web application.
- **Kafka** – Message broker for real-time streaming of BTC price data.
- **Docker** – Containerization of services for consistent local development and deployment.
- **TimescaleDB** – Time-series optimized PostgreSQL database for storing historical price data.

### Python Libraries
- **Dash** – Used to build the interactive candlestick chart dashboard.
- **gevent** – Provides concurrency support for WebSocket streaming.
- **kafka-python** – Kafka client for Python to produce and consume messages.
- **pandas** – Data manipulation.
- **psycopg2** – PostgreSQL adapter for Python.
- **pydantic** – Data validation using Python type annotations.
- **websocket-client** – WebSocket client to receive live price data from Kraken Websocket API.

## Installation and Setup
This setup guide assumes you have Docker, and Python 3.7.9 installed on your system. For the record I user Powershell, so you might want to adjust the running command accordingly.

1. **Clone the Repository**
   ```powershell
   git clone https://github.com/marcellinus-witarsah/btc-price-dashboard
   cd btc-price-dashboard
   ```

2. **Create a Python Virtual Environment**
   ```powershell
   python3.7 -m venv venv
   venv\Scripts\activate
   ```
   
3. **Install Dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Set PYTHONPATH**
   ```powershell
   $env:PYTHONPATH=$pwd
   ```

5. **Run Kafka Producer**: this script connects to the Kraken WebSocket API and sends price updates to Kafka.
   ```powershell
   python src\kafka\producer\main.py
   ```

6. **Run Kafka Consumer**: this component reads data from Kafka and writes it to TimescaleDB.
   ```powershell
   python src\kafka\consumer\main.py
   ```

7. **Launch Web Application Dashboard**
   ```powershell
   python src\dash\app.py
   ```
