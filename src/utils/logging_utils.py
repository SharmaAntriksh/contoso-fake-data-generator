from datetime import datetime
from contextlib import contextmanager

# Core log function
def log(level, msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {level:<5} | {msg}")

# Convenience helpers
def info(msg): log("INFO", msg)
def skip(msg): log("SKIP", msg)
def work(msg): log("WORK", msg)
def done(msg): log("DONE", msg)

# Stage wrapper for start/end messages
@contextmanager
def stage(label: str):
    info(f"{label}...")
    yield
    done(f"{label}")
