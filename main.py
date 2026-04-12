from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Trading Backend Running"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "symbols": ["NIFTY", "SENSEX"],
        "nifty_count": 100,
        "sensex_count": 100
    }
