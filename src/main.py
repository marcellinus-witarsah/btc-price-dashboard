from kraken_source_data.kraken_source_sync import KrakenSourceSync

if __name__ == "__main__":
    kraken_source = KrakenSourceSync(
        url="wss://ws.kraken.com/v2",
        symbols=["BTC/USD"]
    )
    
    kraken_source.subscribe()
    while True:
        kraken_source.get_trades()