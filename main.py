import csv
import time
import threading
import queue
from collections import deque
import datetime as dt
import mysql.connector

# ============================================================
#               CONFIGURATION PARAMETERS
# ============================================================

HS = 50000          # Max stream tuples living in memory
VP = 2000           # Size of customer partitions
BATCH_SIZE = 5000   # Rows inserted per database batch
MASTER_BATCH_SIZE = 5000
STREAM_DELAY = 0.0  # Seconds to pause between streamed rows (0 for fastest)

LAMBDA_RATE = 1200  # Stream arrival rate (rows/sec)
MU_RATE = 1800      # Service rate, kept for reference

TRANSACTION_FILE = "transactional_data.csv"
CUSTOMER_FILE = "customer_master_data.csv"
PRODUCT_FILE = "product_master_data.csv"

# ============================================================
#               GLOBAL DATA STRUCTURES
# ============================================================

stream_buffer = queue.Queue()
producer_done = False

hash_table = {}          # customer_id -> [stream tuples]
fifo_queue = deque()     # preserves arrival order of customer ids
output_buffer = []       # accumulated fact rows

customer_partitions = {}  # partition_id -> {customer_id -> [rows]}
product_lookup = {}       # product_id -> product row
stats_lock = threading.Lock()
stats = {"streamed": 0, "enriched": 0, "batches": 0}


def log(message: str) -> None:
    timestamp = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def print_banner(title: str) -> None:
    line = "=" * (len(title) + 4)
    print(f"\n{line}\n| {title} |\n{line}")

# ============================================================
#               DATABASE CONNECTION
# ============================================================


def get_db_connection():
    """Connect to MySQL DW schema."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="salarkhan",
        autocommit=True
    )


# ============================================================
#            MASTER DATA LOAD (BATCHED)
# ============================================================


def read_csv_in_batches(path, batch_size):
    """Yield lists of rows from a CSV file."""
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def load_master_data():
    """Load customer and product master data in batches."""
    global customer_partitions, product_lookup
    customer_partitions = {}
    product_lookup = {}

    print_banner("Loading Customers")
    total_customers = 0
    for batch_num, batch in enumerate(
        read_csv_in_batches(CUSTOMER_FILE, MASTER_BATCH_SIZE), start=1
    ):
        batch_count = 0
        for row in batch:
            try:
                cid = int(row["Customer_ID"])
            except (ValueError, TypeError):
                continue
            bucket = cid % VP
            bucket_map = customer_partitions.setdefault(bucket, {})
            bucket_map.setdefault(row["Customer_ID"], []).append(row)
            total_customers += 1
            batch_count += 1
        log(f"Customer batch {batch_num}: {batch_count} rows (total {total_customers})")

    print_banner("Loading Products")
    total_products = 0
    for batch_num, batch in enumerate(
        read_csv_in_batches(PRODUCT_FILE, MASTER_BATCH_SIZE), start=1
    ):
        batch_count = 0
        for row in batch:
            product_lookup[row["Product_ID"]] = row
            total_products += 1
            batch_count += 1
        log(f"Product batch {batch_num}: {batch_count} rows (total {total_products})")

    log("Master data loaded successfully.")
    return total_customers, total_products


def execute_batches(cursor, sql, rows, chunk_size=MASTER_BATCH_SIZE):
    for idx in range(0, len(rows), chunk_size):
        cursor.executemany(sql, rows[idx : idx + chunk_size])


def sync_dimension_tables():
    db = get_db_connection()
    cursor = db.cursor()

    # Customers
    customer_rows = []
    seen_customers = set()
    for bucket_map in customer_partitions.values():
        for cid, rows in bucket_map.items():
            if cid in seen_customers:
                continue
            row = rows[0]
            customer_rows.append(
                (
                    int(cid),
                    row.get("Gender"),
                    row.get("Age"),
                    int(row.get("Occupation") or 0),
                    row.get("City_Category"),
                    row.get("Stay_In_Current_City_Years"),
                    int(row.get("Marital_Status") or 0),
                )
            )
            seen_customers.add(cid)
    if customer_rows:
        execute_batches(
            cursor,
            """
            INSERT INTO dim_customer (
                customer_id,
                gender,
                age_range,
                occupation_code,
                city_category,
                stay_in_city_years,
                marital_status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                gender = VALUES(gender),
                age_range = VALUES(age_range),
                occupation_code = VALUES(occupation_code),
                city_category = VALUES(city_category),
                stay_in_city_years = VALUES(stay_in_city_years),
                marital_status = VALUES(marital_status)
            """,
            customer_rows,
        )

    # Stores and suppliers derived from products
    store_rows = []
    supplier_rows = []
    product_rows = []
    seen_stores = set()
    seen_suppliers = set()
    for product in product_lookup.values():
        store_id = int(product["storeID"])
        supplier_id = int(product["supplierID"])
        if store_id not in seen_stores:
            store_rows.append((store_id, product.get("storeName"), None))
            seen_stores.add(store_id)
        if supplier_id not in seen_suppliers:
            supplier_rows.append((supplier_id, product.get("supplierName")))
            seen_suppliers.add(supplier_id)
        product_rows.append(
            (
                product["Product_ID"],
                product.get("Product_Category"),
                float(product.get("price$") or 0),
                store_id,
                supplier_id,
            )
        )

    if store_rows:
        execute_batches(
            cursor,
            """
            INSERT INTO dim_store (store_id, store_name, city_category)
            VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                store_name = VALUES(store_name),
                city_category = VALUES(city_category)
            """,
            store_rows,
        )

    if supplier_rows:
        execute_batches(
            cursor,
            """
            INSERT INTO dim_supplier (supplier_id, supplier_name)
            VALUES (%s,%s)
            ON DUPLICATE KEY UPDATE supplier_name = VALUES(supplier_name)
            """,
            supplier_rows,
        )

    if product_rows:
        execute_batches(
            cursor,
            """
            INSERT INTO dim_product (
                product_id,
                product_category,
                unit_price,
                store_id,
                supplier_id
            )
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                product_category = VALUES(product_category),
                unit_price = VALUES(unit_price),
                store_id = VALUES(store_id),
                supplier_id = VALUES(supplier_id)
            """,
            product_rows,
        )

    db.commit()
    cursor.close()
    db.close()
    log("Dimension tables synchronized.")


def collect_unique_dates():
    dates = set()
    with open(TRANSACTION_FILE, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("date"):
                dates.add(row["date"])
    return dates


def sync_date_dimension():
    unique_dates = collect_unique_dates()
    if not unique_dates:
        return

    db = get_db_connection()
    cursor = db.cursor()
    date_rows = []
    for date_str in sorted(unique_dates):
        date_obj = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        date_rows.append(
            (
                date_str,
                date_obj,
                date_obj.day,
                date_obj.strftime("%A"),
                date_obj.month,
                date_obj.strftime("%B"),
                (date_obj.month - 1) // 3 + 1,
                date_obj.year,
                1 if date_obj.weekday() >= 5 else 0,
            )
        )

    execute_batches(
        cursor,
        """
        INSERT INTO dim_date (
            date_id,
            full_date,
            day_of_month,
            day_name,
            month_number,
            month_name,
            quarter_number,
            year,
            is_weekend
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            day_of_month = VALUES(day_of_month),
            day_name = VALUES(day_name),
            month_number = VALUES(month_number),
            month_name = VALUES(month_name),
            quarter_number = VALUES(quarter_number),
            year = VALUES(year),
            is_weekend = VALUES(is_weekend)
        """,
        date_rows,
    )
    db.commit()
    cursor.close()
    db.close()
    log("Date dimension synchronized.")


def get_partition_for_key(customer_id):
    """Return precomputed customer partition for a given id."""
    try:
        cid = int(customer_id)
    except (TypeError, ValueError):
        return []
    bucket = cid % VP
    bucket_map = customer_partitions.get(bucket)
    if not bucket_map:
        return []
    return bucket_map.get(str(customer_id), [])


def get_product(product_id):
    return product_lookup.get(product_id)


# ============================================================
#       ETL HELPERS: JOIN + INSERT PREP
# ============================================================


def join_stream_with_master(stream_row, customer_row, product_row):
    """Combine stream, customer, and product attributes."""
    quantity = int(stream_row["quantity"])
    price = float(product_row["price$"])
    return {
        "order_id": stream_row["orderID"],
        "customer_id": stream_row["Customer_ID"],
        "product_id": stream_row["Product_ID"],
        "store_id": product_row["storeID"],
        "supplier_id": product_row["supplierID"],
        "date_id": stream_row["date"],
        "quantity": quantity,
        "unit_price": price,
        "total_amount": quantity * price,
    }


# ============================================================
#               PRODUCER THREAD (EXTRACT)
# ============================================================


def producer_thread():
    global producer_done

    with open(TRANSACTION_FILE, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stream_buffer.put(row)
            with stats_lock:
                stats["streamed"] += 1
                streamed = stats["streamed"]
            if streamed % 50000 == 0:
                log(f"Streamed {streamed:,} transactions...")
            if STREAM_DELAY > 0:
                time.sleep(STREAM_DELAY)

    producer_done = True
    log(f"Producer completed. Total streamed: {stats['streamed']:,}")


# ============================================================
#                 LOAD INTO DATA WAREHOUSE (LOAD)
# ============================================================


def load_batch_to_dw(batch):
    if not batch:
        return
    db = get_db_connection()
    cursor = db.cursor()

    insert_sql = """
        INSERT INTO fact_sales (
            order_id,
            customer_id,
            product_id,
            store_id,
            supplier_id,
            date_id,
            quantity,
            unit_price,
            total_amount
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price),
            total_amount = VALUES(total_amount)
    """

    rows = [
        (
            r["order_id"],
            r["customer_id"],
            r["product_id"],
            r["store_id"],
            r["supplier_id"],
            r["date_id"],
            r["quantity"],
            r["unit_price"],
            r["total_amount"],
        )
        for r in batch
    ]

    cursor.executemany(insert_sql, rows)
    db.commit()
    cursor.close()
    db.close()
    with stats_lock:
        stats["batches"] += 1
    log(f"Inserted batch #{stats['batches']} ({len(batch)} rows)")


# ============================================================
#                HYBRID JOIN THREAD (TRANSFORM)
# ============================================================


def hybrid_join_thread():
    MAX_ROWS = 1000  # Stop after 1000 rows
    
    while True:
        # Check if we've reached the limit
        with stats_lock:
            if stats["enriched"] >= MAX_ROWS:
                log(f"Reached limit of {MAX_ROWS} rows. Stopping hybrid join.")
                break
        
        while len(hash_table) < HS and not stream_buffer.empty():
            stream_row = stream_buffer.get()
            key = stream_row["Customer_ID"]
            if key not in hash_table:
                hash_table[key] = []
                fifo_queue.append(key)
            hash_table[key].append(stream_row)

        if producer_done and not hash_table:
            break

        if not fifo_queue:
            continue

        oldest_key = fifo_queue.popleft()
        partition = get_partition_for_key(oldest_key)

        if oldest_key in hash_table:
            matches = partition
            if not matches:
                del hash_table[oldest_key]
                continue
            for stream_row in hash_table[oldest_key]:
                # Check limit before processing
                with stats_lock:
                    if stats["enriched"] >= MAX_ROWS:
                        break
                
                product_row = get_product(stream_row["Product_ID"])
                if not product_row:
                    continue
                for customer_row in matches:
                    joined = join_stream_with_master(
                        stream_row, customer_row, product_row
                    )
                    output_buffer.append(joined)
                    with stats_lock:
                        stats["enriched"] += 1
                        # Check if we just hit the limit
                        if stats["enriched"] >= MAX_ROWS:
                            break
            del hash_table[oldest_key]

        if len(output_buffer) >= BATCH_SIZE:
            load_batch_to_dw(output_buffer)
            output_buffer.clear()

    # Load remaining rows
    if output_buffer:
        load_batch_to_dw(output_buffer)

    log("Hybrid join completed.")


# ============================================================
#                   MAIN FUNCTION
# ============================================================


def main():
    print_banner("HYBRIDJOIN ETL")
    total_customers, total_products = load_master_data()
    print_banner("Syncing Dimensions")
    sync_dimension_tables()
    sync_date_dimension()
    log(
        f"Customers cached: {total_customers:,} | Products cached: {total_products:,}"
    )
    print_banner("Streaming & Joining")
    t_stream = threading.Thread(target=producer_thread, daemon=True)
    t_join = threading.Thread(target=hybrid_join_thread, daemon=True)

    t_stream.start()
    t_join.start()

    t_stream.join()
    t_join.join()

    print_banner("ETL SUMMARY")
    with stats_lock:
        log(f"Streamed rows    : {stats['streamed']:,}")
        log(f"Enriched rows    : {stats['enriched']:,}")
        log(f"Batches inserted : {stats['batches']:,}")
    log("ETL completed successfully!")


# ============================================================
#                       RUN SCRIPT
# ============================================================


if __name__ == "__main__":
    main()
