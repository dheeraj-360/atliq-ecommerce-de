"""
AtliQ Commerce — Phase 2 (LEARNER STARTER)
Live Order Event Producer — stream simulated order events to Kafka.

YOUR JOB: complete the TODOs. The event shapes and helper pieces are given so
you focus on the Kafka parts: configuring the producer and publishing events.

Prereqs:
    pip install confluent-kafka python-dotenv
.env (next to this file — copy .env.example):
    KAFKA_BOOTSTRAP=pkc-xxxxx.region.provider.confluent.cloud:9092
    KAFKA_API_KEY=...
    KAFKA_API_SECRET=...
    KAFKA_TOPIC=atliq.orders.events

Run:  python order_event_producer.py --rate 2 --duration 300

order_ids continue from where the previous run stopped (saved in a small
.last_order_id file next to this script), so two runs never reuse an order_id.

Like a real event pipeline, a small share of events arrive messy: replayed
duplicates, records with a missing timestamp or an unparseable amount, and late
events (some a few minutes late, some later than your 10-minute watermark).
Your Silver stream must handle them. Pass --clean to switch this off while debugging.
"""
import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
PRODUCT_PRICES = {1: 2499, 2: 3299, 3: 1799, 4: 1499, 5: 4999, 6: 899, 7: 3499, 8: 2199,
                  9: 7999, 10: 6999, 11: 899, 12: 1299, 13: 5499, 14: 3299, 15: 549,
                  16: 1199, 17: 999, 18: 749, 19: 2299, 20: 699, 21: 599, 22: 1599,
                  23: 2999, 24: 799, 25: 1299, 26: 2499, 27: 3499, 28: 4499, 29: 1099,
                  30: 1399, 31: 1899, 32: 799, 33: 499, 34: 649, 35: 399, 36: 549,
                  37: 1299, 38: 449, 39: 299, 40: 349, 41: 249, 42: 449, 43: 599, 44: 399,
                  45: 199, 46: 349, 47: 799, 48: 449, 49: 1999, 50: 899, 51: 799, 52: 1899,
                  53: 2499, 54: 699, 55: 249, 56: 1299, 57: 599, 58: 1499, 59: 399, 60: 299}
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".last_order_id")


def load_next_order_id(default: int = 100_000) -> int:
    """Continue order_id numbering from the previous run (first run starts at 100000)."""
    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return default


def save_next_order_id(next_order_id: int) -> None:
    with open(STATE_FILE, "w") as f:
        f.write(str(next_order_id))


MESS = dict(replay=0.02, bad_record=0.01, late=0.02, very_late=0.01)


def add_realistic_mess(events: list, clean: bool) -> list:
    """Make a few events arrive the way real ones do. Returns the list to publish."""
    if clean:
        return events
    out = []
    for ev in events:
        ev = dict(ev)
        u = random.random()
        if u < MESS["bad_record"]:                        # a broken record from an old app version
            if random.random() < 0.5:
                ev.pop("event_ts")
            else:
                ev["order_amount"] = "N/A"
        elif u < MESS["bad_record"] + MESS["late"]:       # late, but inside the 10-minute watermark
            ev["event_ts"] = shift_ts(ev["event_ts"], random.randint(120, 360))
        elif u < MESS["bad_record"] + MESS["late"] + MESS["very_late"]:   # later than the watermark
            ev["event_ts"] = shift_ts(ev["event_ts"], random.randint(900, 1500))
        out.append(ev)
        if random.random() < MESS["replay"]:              # the same event delivered twice
            out.append(dict(ev))
    return out


def shift_ts(ts: str, seconds_back: int) -> str:
    t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    t = t.timestamp() - seconds_back
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"



def make_producer() -> Producer:
    config = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    }
    return Producer(config)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def base_event(event_type: str, order: dict) -> dict:
    """One event. event_id is your de-duplication key in Silver — keep it."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,      # order_placed | payment_received | order_shipped | order_cancelled
        "event_ts": now_iso(),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "city": order["city"],
        "product_id": order["product_id"],
        "quantity": order["quantity"],
        "order_amount": order["order_amount"],
        "payment_method": order["payment_method"] if event_type == "payment_received" else None,
    }


def new_order(order_id: int) -> dict:
    product_id = random.choice(list(PRODUCT_PRICES))
    qty = random.randint(1, 3)
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 1000),
        "city": random.choice(CITIES),
        "product_id": product_id,
        "quantity": qty,
        "order_amount": PRODUCT_PRICES[product_id] * qty,
        "payment_method": random.choice(METHODS),
    }


def run(rate: float, duration: int, clean: bool = False):
    producer = make_producer()
    topic = os.environ.get("KAFKA_TOPIC", "atliq.orders.events")
    open_orders, next_order_id, sent = [], load_next_order_id(), 0
    deadline = time.time() + duration

    print(f"Producing to '{topic}' at ~{rate} events/sec for {duration}s ...")
    try:
        while time.time() < deadline:
            roll = random.random()
            if roll < 0.55 or not open_orders:
                order = new_order(next_order_id); next_order_id += 1
                events = [base_event("order_placed", order)]
                if order["payment_method"] != "COD":
                    events.append(base_event("payment_received", order))
                open_orders.append(order)
            elif roll < 0.90:
                events = [base_event("order_shipped", open_orders.pop(random.randrange(len(open_orders))))]
            else:
                events = [base_event("order_cancelled", open_orders.pop(random.randrange(len(open_orders))))]

            events = add_realistic_mess(events, clean)


            for ev in events:
                def delivery_callback(err, msg):
                    if err is not None:
                        print(f"Delivery failed: {err}")
                    else:
                        print(
                            f"Delivered to {msg.topic()} "
                            f"partition={msg.partition()} "
                            f"offset={msg.offset()}"
                        )

                producer.produce(
                    topic=topic,
                    key=str(ev["order_id"]),
                    value=json.dumps(ev),
                    callback=delivery_callback,
                )
                sent += 1

            producer.poll(0)
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("\nStopping ...")

    
    finally:
        remaining = producer.flush(30)
        if remaining:
            print(f"Warning: {remaining} messages not delivered")
        save_next_order_id(next_order_id)
        print(f"Done. {sent} events queued.")
        

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--clean", action="store_true", help="send only well-formed, on-time events")
    args = ap.parse_args()
    run(args.rate, args.duration, args.clean)
