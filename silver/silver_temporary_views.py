# Databricks notebook source
# DBTITLE 1,Importing all the necessary modules
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, coalesce, when, trim, initcap, lower,
    to_date, regexp_replace, from_json, get_json_object,
    nanvl, lit, upper, split, transform, filter,
    array_distinct, size, array_join
)
from pyspark.sql.types import StringType, MapType, DoubleType

# COMMAND ----------

# DBTITLE 1,customer bronze view with unified columns and normalization

US_STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]

@dp.temporary_view(
    name="customers_bronze_view",
    comment="Standardised, cleaned and deduplicated customers: unified column names, R4 city/state swap fixed, segment/region normalised, quarantined records filtered, most complete record kept per customer_id."
)
def customers_bronze_view():
    from pyspark.sql.functions import max_by, struct
    
    df = spark.readStream.table("customers").select(
        # Unify the four ID column variants into one canonical column
        coalesce(
            col("customer_id"),
            col("CustomerID"),
            col("cust_id"),
            col("customer_identifier")
        ).alias("customer_id"),

        # Unify the two email column variants
        coalesce(
            col("customer_email"),
            col("email_address")
        ).alias("customer_email"),

        # Unify the two name column variants
        coalesce(
            col("customer_name"),
            col("full_name")
        ).alias("customer_name"),

        # Unify the two segment column variants (raw — normalised below)
        coalesce(
            col("segment"),
            col("customer_segment")
        ).alias("segment"),

        col("country"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("region"),
        col("source_region"),
        col("_rescued_data"),
        col("_source_file_path"),
        col("_source_file_name"),
        col("_source_modified_time"),
        col("_ingested_at"),
    )

    # ── R4 city/state swap fix ─────────────────────────────────────────────
    # Region 4 has the city and state columns physically interchanged in the
    # source file. We detect this by checking whether the city column contains
    # a value that is actually a US state name. When it does, swap them back.
    city_is_state = col("city").isin(US_STATES)
    df = df.withColumn("city_fixed",
        when(city_is_state, col("state")).otherwise(col("city"))
    ).withColumn("state_fixed",
        when(city_is_state, col("city")).otherwise(col("state"))
    ).drop("city", "state") \
     .withColumnRenamed("city_fixed", "city") \
     .withColumnRenamed("state_fixed", "state")

    # ── Segment normalisation ──────────────────────────────────────────────
    # Abbreviations and typos found during Bronze analysis mapped to the three
    # canonical values that the Silver expectation rule allows.
    df = df.withColumn("segment",
        when(col("segment").isin("CONS", "Consumer", "Cosumer"), "Consumer")
       .when(col("segment").isin("CORP", "Corporate"),           "Corporate")
       .when(col("segment").isin("HO", "Home Office"),           "Home Office")
       .otherwise(col("segment"))   # unknown values surface in quarantine
    )

    # ── Region normalisation ───────────────────────────────────────────────
    df = df.withColumn("region",
        when(col("region").isin("N", "North"),    "North")
       .when(col("region").isin("S", "South"),    "South")
       .when(col("region").isin("E", "East"),     "East")
       .when(col("region").isin("W", "West"),     "West")
       .when(col("region").isin("C", "Central"),  "Central")
       .otherwise(col("region"))
    )

    # ── Completeness score for deduplication ───────────────────────────────
    # Email weighted higher (2) because it's critical for customer identification.
    df = df.withColumn("_completeness_score",
        when(col("customer_email").isNotNull(), 2).otherwise(0) +
        when(col("customer_name").isNotNull(),  1).otherwise(0) +
        when(col("segment").isNotNull(),        1).otherwise(0) +
        when(col("region").isNotNull(),         1).otherwise(0)
    )

    # ── Filter out quarantined records ─────────────────────────────────────
    # Quarantine rules: records must pass ALL of these to be included
    quarantine_rules = (
        "customer_id IS NOT NULL AND "
        "customer_name IS NOT NULL AND TRIM(customer_name) != '' AND "
        "segment IN ('Home Office','Corporate','Consumer') AND "
        "region IN ('North','South','East','West','Central')"
    )
    df = df.filter(quarantine_rules)

    # ── Deduplicate: keep the most complete record per customer_id ─────────
    # max_by selects the struct with the highest _completeness_score
    df = df.groupBy("customer_id").agg(
        max_by(
            struct(
                col("customer_email"),
                col("customer_name"),
                col("segment"),
                col("country"),
                col("city"),
                col("state"),
                col("postal_code"),
                col("region"),
                col("source_region")
            ),
            col("_completeness_score")
        ).alias("best_record")
    )

    # Expand the struct back into columns
    return df.select(
        col("customer_id"),
        col("best_record.customer_email"),
        col("best_record.customer_name"),
        col("best_record.segment"),
        col("best_record.country"),
        col("best_record.city"),
        col("best_record.state"),
        col("best_record.postal_code"),
        col("best_record.region"),
        col("best_record.source_region")
    )

# COMMAND ----------

# DBTITLE 1,products bronze view with sizes cleaned and categories  ...
@dp.temporary_view(
    name="products_bronze_view",
    comment="Standardised Bronze products: sizes cleaned, dates parsed, category mapped."
)
def products_bronze_view():
    df = spark.readStream.table("globalmart_dev.bronze.products")

    # ── Step 1: Map full words to standard size codes ──────────────────────
    # Must happen BEFORE splitting so "Extra Large" (two words) is caught.
    # Large must be last — replace Extra Large and X-Large first otherwise
    # "Extra Large" becomes "Extra L" before the full word is matched.
    word_to_code = [
        (r"(?i)\bExtra Large\b", "XL"),
        (r"(?i)\bExtra Small\b", "XS"),
        (r"(?i)\bX-Large\b",     "XL"),
        (r"(?i)\bX-Small\b",     "XS"),
        (r"(?i)\bMedium\b",      "M"),
        (r"(?i)\bSmall\b",       "S"),
        (r"(?i)\bLarge\b",       "L"),
    ]
    sizes_col = col("sizes")
    for pattern, replacement in word_to_code:
        sizes_col = regexp_replace(sizes_col, pattern, replacement)

    df = df.withColumn("sizes",
        when(col("sizes").isNull(), lit(None)).otherwise(sizes_col)
    )

    # ── Step 2: Split on comma into array ─────────────────────────────────
    df = df.withColumn("sizes_array", split(col("sizes"), ","))

    # ── Step 3: Trim each token and normalise "10 M" → "10M" ──────────────
    # Some tokens have a space between the number and width code.
    # Remove that space so all tokens follow the same pattern.
    df = df.withColumn("sizes_array",
        transform(col("sizes_array"),
            lambda s: regexp_replace(trim(s), r"(\d)\s+([A-Za-z])", "$1$2")
        )
    )

    # ── Step 4: Filter — keep only valid size tokens ───────────────────────
    # Valid patterns:
    #   Pure numeric:          8, 9.5, 40, 44.5
    #   Numeric + width code:  10M, 8.5W, 9N, 156E, 4W
    #   Letter sizes:          S, M, L, XL, XXL, XS
    #   Extended sizes:        2X, 3X, 4X
    # Anything else (URLs, long strings, special chars) is dropped silently.
    VALID = (
        r"^("
        r"\d+(\.\d+)?[WwMmNnEe]?\d*[WwMmNnEe]?"
        r"|[Xx]{0,2}[SsLlMm]"
        r"|\d+[Xx]"
        r")$"
    )
    df = df.withColumn("sizes_array",
        filter(col("sizes_array"),
            lambda s: (trim(s) != "") & s.rlike(VALID)
        )
    )

    # ── Step 5: Remove duplicates within the array ─────────────────────────
    df = df.withColumn("sizes_array", array_distinct(col("sizes_array")))

    # ── Step 6: Rejoin to comma string — empty array becomes "One Size" ────
    df = df.withColumn("sizes",
        when(
            col("sizes_array").isNull() | (size(col("sizes_array")) == 0),
            lit("One Size")
        ).otherwise(array_join(col("sizes_array"), ","))
    ).drop("sizes_array")

    # ── Date parsing ───────────────────────────────────────────────────────
    # Explicit ISO 8601 format — Spark default silently produces null for Z suffix
    df = df.withColumn("dateAdded",
        to_date(col("dateAdded"),   "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn("dateUpdated",
        to_date(col("dateUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # ── Category classification ────────────────────────────────────────────
    # Women before Men — "women" contains "men" as substring
    df = df.withColumn("category",
        when(lower(col("categories")).like("%women%"),    "Women")
       .when(lower(col("categories")).like("%men%"),      "Men")
       .when(lower(col("categories")).like("%shoe%"),     "Shoes")
       .when(lower(col("categories")).like("%clothing%"), "Clothing")
       .otherwise("Other")
    )

    # ── Colors null fill ───────────────────────────────────────────────────
    df = df.withColumn("colors",
        when(col("colors").isNull(), lit("Not Specified")).otherwise(col("colors"))
    )

    return df

# COMMAND ----------

# DBTITLE 1,orders bronze view with date parsing and status normali ...
@dp.temporary_view(
    name="orders_bronze_view",
    comment="Standardised Bronze orders: all date formats handled robustly, status and ship_mode normalised."
)
def orders_bronze_view():
    df = spark.readStream.table("orders")

    # ── Date parsing — format-agnostic───────────────────────
    # Formats tried in order, most specific first.
    # Adding a new region's format = add one string here, nothing else changes.
    # Any date that fails all formats becomes null — quarantine catches it.
    DATE_FORMATS = [
        "MM/dd/yyyy HH:mm",      # Region 1:   02/21/2018 06:20
        "yyyy-MM-dd HH:mm",      # Region 3,5: 2018-05-04 11:52
        "yyyy-MM-dd HH:mm:ss",   # future:     with seconds
        "MM/dd/yyyy HH:mm:ss",   # future:     R1 style with seconds
        "yyyy-MM-dd",            # fallback:   date only
        "MM/dd/yyyy",            # fallback:   R1 date only
    ]

    date_columns = [
        "order_purchase_date",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for col_name in date_columns:
        df = df.withColumn(col_name,
            coalesce(*[to_date(col(col_name), fmt) for fmt in DATE_FORMATS])
        )

    # ── order_status normalisation ─────────────────────────────────────────
    df = df.withColumn("order_status",
        when(col("order_status").isin("canceled",    "Cancelled"),   "Cancelled")
       .when(col("order_status").isin("created",     "Created"),     "Created")
       .when(col("order_status").isin("delivered",   "Delivered"),   "Delivered")
       .when(col("order_status").isin("invoiced",    "Invoiced"),    "Invoiced")
       .when(col("order_status").isin("processing",  "Processing"),  "Processing")
       .when(col("order_status").isin("shipped",     "Shipped"),     "Shipped")
       .when(col("order_status").isin("unavailable", "Unavailable"), "Unavailable")
       .otherwise(col("order_status"))
    )

    # ── ship_mode normalisation ────────────────────────────────────────────
    df = df.withColumn("ship_mode",
        when(col("ship_mode") == "1st Class",  "First Class")
       .when(col("ship_mode") == "2nd Class",  "Second Class")
       .when(col("ship_mode") == "Std Class",  "Standard Class")
       .otherwise(initcap(trim(col("ship_mode"))))
    )

    return df

# COMMAND ----------

# DBTITLE 1,returns bronze view with refund recovery and date clean ...
@dp.temporary_view(
    name="returns_bronze_view",
    comment="Standardised Bronze returns: schema merged, rescued refund_amount recovered, NaN→null, dates parsed, garbage reasons nulled."
)
def returns_bronze_view():
    df = spark.readStream.table("returns")

    # ── Step 1: Recover refund_amount from _rescued_data (R6 "$" string) ──
    # R6 stored refund_amount as "$130.65". The "$" caused Auto Loader to rescue
    # these rows. We parse the JSON, strip "$", and cast to DOUBLE before COALESCE
    # so the recovered value takes priority over the null in the refund_amount column.
    df = df.withColumn(
        "refund_amount_rescued",
        regexp_replace(
            get_json_object(col("_rescued_data"), "$.refund_amount"),
            r"[$,]", ""
        ).cast(DoubleType())
    )

    # ── Step 2: Unify column name variants into canonical names ───────────
    df = df.select(
        coalesce(col("order_id"),      col("OrderId")).alias("order_id"),
        coalesce(col("return_reason"), col("reason")).alias("return_reason"),
        coalesce(col("return_date"),   col("date_of_return")).alias("return_date"),
        # Prefer the rescued double value, then the raw refund_amount (R6 non-rescued rows),
        # then fall back to the amount column (returns_2.json format)
        coalesce(
            col("refund_amount_rescued"),
            nanvl(col("refund_amount"), lit(None).cast(DoubleType())),
            col("amount")
        ).alias("refund_amount"),
        coalesce(col("return_status"), col("status")).alias("return_status"),
        col("_rescued_data"),
        col("_source_file_path"),
        col("_source_file_name"),
        col("_source_modified_time"),
        col("_ingested_at"),
    )

    # ── Step 3: Parse return_date — handle "NULL" string and two date formats
    # First null out the literal "NULL" string so to_date does not choke on it.
    # Then try YYYY-MM-DD (ISO, returns_2.json) then DD-MM-YYYY (returns_2.json variant).
    df = df.withColumn("return_date",
        when(upper(trim(col("return_date"))) == "NULL", lit(None))
        .otherwise(col("return_date"))
    )
    df = df.withColumn("return_date",
        coalesce(
            to_date(col("return_date"), "yyyy-MM-dd"),
            to_date(col("return_date"), "dd-MM-yyyy"),
            to_date(col("return_date"), "MM/dd/yyyy"),
        )
    )

    # ── Step 4: Garbage reason values → SQL null ──────────────────────────
    # "?" and the literal string "NULL" both mean unknown — treat as null
    # so Silver rules and Gold aggregations handle them consistently.
    df = df.withColumn("return_reason",
        when(
            col("return_reason").isNull() |
            (trim(col("return_reason")) == "?") |
            (upper(trim(col("return_reason"))) == "NULL"),
            lit(None)
        ).otherwise(trim(col("return_reason")))
    )

    return df

# COMMAND ----------

# DBTITLE 1,transactions bronze view with id recovery and discount
@dp.temporary_view(
    name="transactions_bronze_view",
    comment=(
        "Standardised Bronze transactions: "
        "Order_ID/Product_ID recovered from _rescued_data for R3, "
        "original Order_id column used as fallback for R2/R4, "
        "R4 percentage discount normalised to decimal, "
        "all numeric columns cast to correct types."
    )
)
def transactions_bronze_view():
    df = spark.readStream.table("transactions")

    # ── Step 1: Recover Order_ID and Product_ID ────────────────────────────
    # Parse _rescued_data JSON into a map so we can extract keys by name.
    # rescued_map is null for rows that have no rescued data (R2, R4) —
    # coalesce falls through to the original Bronze column for those rows.
    df = df.withColumn(
        "rescued_map",
        from_json(col("_rescued_data"), MapType(StringType(), StringType()))
    )

    df = df.withColumn("order_id",
        coalesce(
            col("rescued_map")["Order_ID"],   # R3: capital D in rescued JSON
            col("rescued_map")["Order_id"],   # R3: lowercase d variant
            col("Order_id"),                  # R2, R4: value already in column
            lit(None).cast(StringType())
        )
    ).withColumn("product_id",
        coalesce(
            col("rescued_map")["Product_ID"],  # R3: capital D in rescued JSON
            col("rescued_map")["Product_id"],  # R3: lowercase d variant
            col("Product_id"),                 # R2, R4: value already in column
            lit(None).cast(StringType())
        )
    ).drop("rescued_map")

    # ── Step 2: Discount normalisation ────────────────────────────────────
    # Region 4 uses "40%" string; other regions use decimal "0.4".
    # After normalisation all regions produce a consistent 0.0–1.0 double.
    # Any value outside 0–1 after normalisation is genuinely invalid and
    # will be caught by the valid_discount quarantine rule.
    df = df.withColumn("discount",
        when(
            col("discount").rlike(r"%$"),
            regexp_replace(col("discount"), "%", "").cast(DoubleType()) / 100
        ).otherwise(
            col("discount").cast(DoubleType())
        )
    )
    df = df.withColumn("profit",
        coalesce(col("profit").cast(DoubleType()), lit(0))
    )
    # ── Step 3: Cast remaining numeric string columns ──────────────────────
    # Sales and Quantity landed as STRING due to Auto Loader type inference.
    # payment_installments also needs int casting.
    df = df.withColumn("Sales",               col("Sales").cast(DoubleType())) \
           .withColumn("Quantity",             col("Quantity").cast("int")) \
           .withColumn("payment_installments", col("payment_installments").cast("int"))

    # ── Step 4: Rename to canonical lowercase column names ─────────────────
    # Bronze schema has "Sales" and "Quantity" with capital first letter.
    # Silver rules use lowercase — rename here so rules never have casing issues.
    df = df.withColumnRenamed("Sales",    "sales") \
           .withColumnRenamed("Quantity", "quantity")

    return df