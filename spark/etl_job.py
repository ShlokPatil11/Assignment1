import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ─────────────────────────────────────────────
# Logging Configuration
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("employee_etl")


def log_count(df, label: str) -> int:
    count = df.count()
    log.info(f"[{label}] Row count: {count}")
    return count


def run_etl():
    # ─────────────────────────────────────────────
    # Step 0 – Spark Session
    # ─────────────────────────────────────────────
    try:
        spark = (
            SparkSession.builder.appName("EmployeeETL")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        log.info("SparkSession started successfully")
    except Exception as e:
        log.error(f"Failed to create SparkSession: {e}")
        sys.exit(1)

    # ─────────────────────────────────────────────
    # Step 1 – Read Raw CSV
    # ─────────────────────────────────────────────
    try:
        raw_path = "/opt/bitnami/spark/data/employees_raw.csv"
        log.info(f"Reading raw CSV from: {raw_path}")
        df_raw = spark.read.csv(raw_path, header=True, inferSchema=False)
        log_count(df_raw, "Step 1 – Raw ingested")
    except Exception as e:
        log.error(f"Step 1 failed – could not read CSV: {e}")
        spark.stop()
        sys.exit(1)

    # ─────────────────────────────────────────────
    # Step 2 – Data Quality
    # ─────────────────────────────────────────────
    try:
        # 2a. Drop duplicates on employee_id
        df_deduped = df_raw.dropDuplicates(["employee_id"])
        log_count(df_deduped, "Step 2a – After dedup on employee_id")

        # 2b. Filter rows with missing required fields
        df_required = df_deduped.filter(
            F.col("first_name").isNotNull() & (F.trim(F.col("first_name")) != "")
            & F.col("last_name").isNotNull() & (F.trim(F.col("last_name")) != "")
            & F.col("email").isNotNull() & (F.trim(F.col("email")) != "")
            & F.col("hire_date").isNotNull() & (F.trim(F.col("hire_date")) != "")
        )
        log_count(df_required, "Step 2b – After dropping missing required fields")

        # 2c. Validate email format with regex
        email_regex = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
        df_valid_email = df_required.filter(F.col("email").rlike(email_regex))
        log_count(df_valid_email, "Step 2c – After removing invalid emails")

        # 2d. Flag and nullify future hire_dates
        today = F.current_date()
        df_hire_flagged = df_valid_email.withColumn(
            "hire_date_future_flag",
            F.when(F.to_date(F.col("hire_date")) > today, True).otherwise(False),
        ).withColumn(
            "hire_date",
            F.when(F.to_date(F.col("hire_date")) > today, None)
             .otherwise(F.to_date(F.col("hire_date"))),
        )
        future_count = df_hire_flagged.filter(F.col("hire_date_future_flag")).count()
        log.info(f"[Step 2d] Future hire_dates flagged and set to null: {future_count}")

    except Exception as e:
        log.error(f"Step 2 (Data Quality) failed: {e}")
        spark.stop()
        sys.exit(1)

    # ─────────────────────────────────────────────
    # Step 3 – Transformations
    # ─────────────────────────────────────────────
    try:
        df_clean = (
            df_hire_flagged
            # Standardize name casing
            .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
            .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
            # full_name: concatenate first_name + last_name
            .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
            # Lowercase email and extract email_domain after "@"
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn(
                "email_domain",
                F.when(
                    F.col("email").contains("@"),
                    F.element_at(F.split(F.col("email"), "@"), 2),
                ).otherwise(None),
            )
            # Clean salary: strip "$" and "," then cast to decimal
            .withColumn(
                "salary",
                F.regexp_replace(F.col("salary"), r"[\$,]", "").cast(DecimalType(12, 2)),
            )
            # salary_band: Junior / Mid / Senior
            .withColumn(
                "salary_band",
                F.when(F.col("salary") < 50000, "Junior")
                 .when((F.col("salary") >= 50000) & (F.col("salary") <= 80000), "Mid")
                 .otherwise("Senior"),
            )
            # Standardize department casing
            .withColumn("department", F.initcap(F.trim(F.col("department"))))
            # Age in years from birth_date
            .withColumn(
                "age",
                F.floor(F.datediff(today, F.to_date(F.col("birth_date"))) / 365.25).cast("integer"),
            )
            # tenure_years from hire_date (null-safe)
            .withColumn(
                "tenure_years",
                F.when(
                    F.col("hire_date").isNotNull(),
                    F.floor(F.datediff(today, F.col("hire_date")) / 365.25).cast("integer"),
                ).otherwise(None),
            )
            # Audit timestamps
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
        )

        log_count(df_clean, "Step 3 – After all transformations")

        # Select final columns matching schema
        df_final = df_clean.select(
            F.col("employee_id").cast("integer"),
            "full_name",
            "email",
            "email_domain",
            "job_title",
            "department",
            "salary",
            "salary_band",
            F.col("manager_id").cast("integer"),
            "address",
            "city",
            "state",
            "zip_code",
            "status",
            "age",
            "tenure_years",
            "created_at",
            "updated_at",
        )

        log_count(df_final, "Step 3 – Final clean dataset ready for load")

    except Exception as e:
        log.error(f"Step 3 (Transformations) failed: {e}")
        spark.stop()
        sys.exit(1)

    # ─────────────────────────────────────────────
    # Step 4 – Write to PostgreSQL via JDBC
    # ─────────────────────────────────────────────
    try:
        jdbc_url = "jdbc:postgresql://postgres:5432/employeedb"
        db_properties = {
            "user":     "postgres",
            "password": "postgres",
            "driver":   "org.postgresql.Driver",
        }

        log.info(f"Writing to PostgreSQL at {jdbc_url} → table: employees_clean")
        df_final.write.jdbc(
            url=jdbc_url,
            table="employees_clean",
            mode="overwrite",
            properties=db_properties,
        )
        log.info("ETL complete – data successfully written to employees_clean")
    except Exception as e:
        log.error(f"Step 4 (PostgreSQL write) failed: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    run_etl()
