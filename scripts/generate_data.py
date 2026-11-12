"""
Generate two daily drops of flight-booking data so the warehouse can show
incremental loading and SCD Type 2 history.

Day 1 (2026-01-01): full customer list, airports, flights, first bookings.
Day 2 (2026-01-02): some customers changed city or tier (-> SCD2),
                    new bookings, and status updates to day-1 bookings (-> MERGE upsert).

Usage: python scripts/generate_data.py [--bookings 3000]
Output: data/raw/<entity>/<entity>_<date>.csv
"""
import argparse
import csv
import random
from pathlib import Path

AIRPORTS = [("MAA", "Chennai", "India"), ("BLR", "Bengaluru", "India"), ("DEL", "Delhi", "India"),
            ("BOM", "Mumbai", "India"), ("HYD", "Hyderabad", "India"), ("COK", "Kochi", "India"),
            ("DXB", "Dubai", "UAE"), ("SIN", "Singapore", "Singapore")]
AIRLINES = ["IndiGo", "Air India", "Vistara", "SpiceJet", "Emirates", "Singapore Airlines"]
CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Mumbai", "Pune", "Delhi", "Kochi"]
TIERS = ["BLUE", "SILVER", "GOLD"]


def write(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main(n_bookings: int, seed: int, out: Path) -> None:
    rnd = random.Random(seed)
    d1, d2 = "2026-01-01", "2026-01-02"

    write(out / "airports" / f"airports_{d1}.csv", ["airport_code", "city", "country"], AIRPORTS)

    flights = []
    for i in range(1, 61):
        o, d = rnd.sample(AIRPORTS, 2)
        flights.append([f"FL{i:03d}", rnd.choice(AIRLINES), o[0], d[0], rnd.randint(60, 420)])
    write(out / "flights" / f"flights_{d1}.csv",
          ["flight_id", "airline", "origin", "destination", "duration_min"], flights)

    customers = {f"CU{i:04d}": [f"CU{i:04d}", f"Passenger {i}", rnd.choice(CITIES), rnd.choice(TIERS)]
                 for i in range(1, 501)}
    write(out / "customers" / f"customers_{d1}.csv",
          ["customer_id", "full_name", "city", "loyalty_tier"], customers.values())

    # Day 2: 40 customers change city or tier, 20 brand-new customers
    changed = rnd.sample(list(customers), 40)
    day2 = []
    for cid in changed:
        c = list(customers[cid])
        if rnd.random() < 0.5:
            c[2] = rnd.choice([x for x in CITIES if x != c[2]])
        else:
            c[3] = rnd.choice([x for x in TIERS if x != c[3]])
        day2.append(c)
    for i in range(501, 521):
        day2.append([f"CU{i:04d}", f"Passenger {i}", rnd.choice(CITIES), "BLUE"])
    write(out / "customers" / f"customers_{d2}.csv",
          ["customer_id", "full_name", "city", "loyalty_tier"], day2)

    def booking(i, day, max_cust):
        f = rnd.choice(flights)
        return [f"BK{i:06d}", f"CU{rnd.randint(1, max_cust):04d}", f[0], day,
                f"2026-0{rnd.randint(1, 3)}-{rnd.randint(1, 28):02d}",
                rnd.choice(["ECONOMY", "ECONOMY", "PREMIUM", "BUSINESS"]),
                round(rnd.uniform(2500, 45000), 2), "CONFIRMED", day]

    header = ["booking_id", "customer_id", "flight_id", "booking_date", "travel_date", "cabin", "fare", "status", "last_updated"]
    half = n_bookings // 2
    b1 = [booking(i, d1, 500) for i in range(1, half + 1)]
    b2 = [booking(i, d2, 520) for i in range(half + 1, n_bookings + 1)]
    for b in rnd.sample(b1, 60):                     # day-1 bookings updated on day 2
        upd = list(b)
        upd[7] = rnd.choice(["CANCELLED", "CHECKED_IN"])
        upd[8] = d2
        b2.append(upd)
    b2 += [list(x) for x in rnd.sample(b2, 10)]      # duplicates in the day-2 drop
    write(out / "bookings" / f"bookings_{d1}.csv", header, b1)
    write(out / "bookings" / f"bookings_{d2}.csv", header, b2)
    print(f"flights={len(flights)} customers d1=500 d2={len(day2)} bookings d1={len(b1)} d2={len(b2)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bookings", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[1] / "data" / "raw")
    a = ap.parse_args()
    main(a.bookings, a.seed, a.out)
