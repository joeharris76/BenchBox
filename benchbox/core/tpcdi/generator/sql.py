"""SQL-based data generation for TPC-DI."""

from __future__ import annotations

import logging
import time
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class TPCDISQLGenerator:
    """High-performance TPC-DI data generator using DuckDB SQL for vectorized operations."""

    def __init__(
        self,
        scale_factor: float = 1.0,
        connection: Any | None = None,
        enable_progress: bool = True,
    ):
        """Initialize the SQL-based TPC-DI data generator.

        Args:
            scale_factor: Scale factor for data generation (1.0 = standard size)
            connection: DuckDB connection to use (creates temporary one if None)
            enable_progress: Enable progress logging
        """
        if duckdb is None:
            raise ImportError("DuckDB is required for SQL-based generation")

        self.scale_factor = scale_factor
        self.connection = connection
        self.temp_connection = None
        self.enable_progress = enable_progress

        # Initialize logger
        self.logger = logging.getLogger(self.__class__.__name__)

        # Base sizes for scale_factor = 1.0
        self.base_customers = 50000
        self.base_companies = 1000
        self.base_securities = 10000
        self.base_accounts = 100000
        self.base_trades = 1000000

        # Generation statistics
        self.generation_stats: dict[str, Any] = {
            "records_generated": 0,
            "tables_generated": 0,
            "generation_times": {},
            "sql_operations": 0,
        }

    def _get_connection(self) -> Any:
        """Get DuckDB connection, creating temporary one if needed."""
        if self.connection is not None:
            return self.connection

        if self.temp_connection is None:
            self.temp_connection = duckdb.connect(":memory:")
        return self.temp_connection

    def _close_temp_connection(self):
        """Close temporary connection if created."""
        if self.temp_connection is not None:
            self.temp_connection.close()
            self.temp_connection = None

    def _setup_lookup_tables(self, conn: Any) -> None:
        """Create lookup tables for random data selection."""
        if self.enable_progress:
            self.logger.info("Setting up lookup tables for random data generation...")

        # First names lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_first_names AS
            SELECT unnest([
                'John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Maria',
                'James', 'Jennifer', 'William', 'Patricia', 'Richard', 'Linda', 'Joseph',
                'Barbara', 'Thomas', 'Elizabeth', 'Christopher', 'Helen', 'Charles', 'Sandra',
                'Daniel', 'Donna', 'Matthew', 'Carol', 'Anthony', 'Ruth', 'Mark', 'Sharon',
                'Donald', 'Michelle', 'Steven', 'Laura', 'Paul', 'Sarah', 'Andrew', 'Kimberly'
            ]) as name, row_number() OVER () as id
        """)

        # Last names lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_last_names AS
            SELECT unnest([
                'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
                'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
                'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker'
            ]) as name, row_number() OVER () as id
        """)

        # Industries lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_industries AS
            SELECT unnest([
                'Technology', 'Healthcare', 'Financial Services', 'Manufacturing',
                'Retail', 'Energy', 'Telecommunications', 'Transportation',
                'Real Estate', 'Media', 'Utilities', 'Consumer Goods',
                'Aerospace', 'Automotive', 'Biotechnology', 'Construction'
            ]) as industry, row_number() OVER () as id
        """)

        # Company names lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_company_names AS
            SELECT unnest([
                'TechCorp', 'GlobalInc', 'InnovativeSoft', 'DataSystems', 'CloudTech',
                'SecureNet', 'SmartSolutions', 'DigitalWorks', 'InfoTech', 'CyberCorp',
                'NextGen', 'PowerSystems', 'EliteServices', 'PrimeData', 'CoreTech',
                'VisionSoft', 'AlphaWorks', 'BetaSystems', 'GammaTech', 'DeltaCorp'
            ]) as name, row_number() OVER () as id
        """)

        # SP Ratings lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_sp_ratings AS
            SELECT unnest([
                'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-',
                'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-'
            ]) as rating, row_number() OVER () as id
        """)

        # Trade types lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_trade_types AS
            SELECT unnest([
                'Market Buy', 'Market Sell', 'Limit Buy', 'Limit Sell',
                'Stop Buy', 'Stop Sell', 'Stop Limit Buy', 'Stop Limit Sell'
            ]) as type, row_number() OVER () as id
        """)

        # Status values lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_statuses AS
            SELECT unnest(['Active', 'Inactive', 'Suspended']) as status, row_number() OVER () as id
        """)

        # US states lookup
        conn.execute("""
            CREATE TEMP TABLE IF NOT EXISTS lookup_us_states AS
            SELECT unnest([
                'CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
                'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI'
            ]) as state, row_number() OVER () as id
        """)

        if self.enable_progress:
            self.logger.info("✅ Lookup tables created")

    def generate_date_dimension(self, conn: Any, start_year: int = 2020, end_year: int = 2024) -> int:
        """Generate date dimension using SQL."""
        if self.enable_progress:
            self.logger.info(f"Generating date dimension ({start_year}-{end_year})...")

        start_time = time.time()

        # Generate all dates in range using SQL
        conn.execute(f"""
            INSERT INTO DimDate
            SELECT
                row_number() OVER (ORDER BY date_val) as SK_DateID,
                date_val as DateValue,
                strftime(date_val, '%Y-%m-%d') as DateDesc,
                EXTRACT(year FROM date_val) as CalendarYearID,
                CAST(EXTRACT(year FROM date_val) AS VARCHAR) as CalendarYearDesc,
                EXTRACT(quarter FROM date_val) as CalendarQtrID,
                'Q' || EXTRACT(quarter FROM date_val) || ' ' || EXTRACT(year FROM date_val) as CalendarQtrDesc,
                EXTRACT(month FROM date_val) as CalendarMonthID,
                strftime(date_val, '%B %Y') as CalendarMonthDesc,
                EXTRACT(week FROM date_val) as CalendarWeekID,
                'Week ' || EXTRACT(week FROM date_val) || ' ' || EXTRACT(year FROM date_val) as CalendarWeekDesc,
                EXTRACT(dow FROM date_val) + 1 as DayOfWeekNum,
                strftime(date_val, '%A') as DayOfWeekDesc,
                EXTRACT(year FROM date_val) as FiscalYearID,  -- Simplified: same as calendar
                'FY ' || EXTRACT(year FROM date_val) as FiscalYearDesc,
                EXTRACT(quarter FROM date_val) as FiscalQtrID,
                'FY ' || EXTRACT(year FROM date_val) || ' Q' || EXTRACT(quarter FROM date_val) as FiscalQtrDesc,
                CASE WHEN EXTRACT(dow FROM date_val) IN (0, 6) THEN true ELSE false END as HolidayFlag
            FROM (
                SELECT (DATE '{start_year}-01-01' + INTERVAL (s.generate_series) DAY) as date_val
                FROM generate_series(0, {(end_year - start_year + 1) * 365 + 10}) s  -- Add extra days for leap years
            ) dates
            WHERE date_val <= DATE '{end_year}-12-31'
        """)

        # Get count of inserted records
        result = conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()
        records_generated = result[0] if result else 0

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += records_generated
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimDate"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {records_generated:,} date records in {generation_time:.3f}s")

        return records_generated

    def generate_time_dimension(self, conn: Any) -> int:
        """Generate time dimension using SQL."""
        if self.enable_progress:
            self.logger.info("Generating time dimension...")

        start_time = time.time()

        # Generate times at 5-minute intervals using SQL
        conn.execute("""
            INSERT INTO DimTime
            SELECT
                row_number() OVER () as SK_TimeID,
                make_time(hour_val, minute_val, 0) as TimeValue,
                hour_val as HourID,
                'Hour ' || lpad(CAST(hour_val AS VARCHAR), 2, '0') as HourDesc,
                minute_val as MinuteID,
                'Minute ' || lpad(CAST(minute_val AS VARCHAR), 2, '0') as MinuteDesc,
                0 as SecondID,
                'Second 00' as SecondDesc,
                CASE
                    WHEN (hour_val = 9 AND minute_val >= 30) OR (hour_val > 9 AND hour_val < 16)
                    THEN true ELSE false
                END as MarketHoursFlag,
                CASE WHEN hour_val >= 8 AND hour_val < 18 THEN true ELSE false END as OfficeHoursFlag
            FROM (
                SELECT
                    h.hour_val,
                    m.minute_val
                FROM
                    (SELECT generate_series as hour_val FROM generate_series(0, 23)) h
                CROSS JOIN
                    (SELECT generate_series * 5 as minute_val FROM generate_series(0, 11)) m
            ) times
            ORDER BY hour_val, minute_val
        """)

        # Get count of inserted records
        result = conn.execute("SELECT COUNT(*) FROM DimTime").fetchone()
        records_generated = result[0] if result else 0

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += records_generated
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimTime"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {records_generated:,} time records in {generation_time:.3f}s")

        return records_generated

    def generate_company_dimension(self, conn: Any) -> int:
        """Generate company dimension using SQL."""
        num_companies = int(self.base_companies * self.scale_factor)

        if self.enable_progress:
            self.logger.info(f"Generating {num_companies:,} company records...")

        start_time = time.time()

        conn.execute(f"""
            INSERT INTO DimCompany
            SELECT
                s.generate_series as SK_CompanyID,
                s.generate_series as CompanyID,
                st.status as Status,
                cn.name || ' ' || s.generate_series as Name,
                ind.industry as Industry,
                rating.rating as SPrating,
                CASE WHEN rating.rating IN ('BBB-', 'BB+', 'BB', 'BB-') THEN true ELSE false END as IsLowGrade,
                'CEO ' || lpad(CAST(s.generate_series AS VARCHAR), 4, '0') as CEO,
                CAST((random() * 9898 + 1)::INTEGER AS VARCHAR) || ' Business Blvd' as AddressLine1,
                CASE WHEN random() < 0.3 THEN 'Suite ' || CAST((random() * 899 + 100)::INTEGER AS VARCHAR) ELSE NULL END as AddressLine2,
                lpad(CAST((random() * 89999 + 10000)::INTEGER AS VARCHAR), 5, '0') as PostalCode,
                'City' || CAST((s.generate_series % 100) AS VARCHAR) as City,
                state.state as StateProv,
                'USA' as Country,
                'Leading company in ' || ind.industry as Description,
                make_date((random() * 70 + 1950)::INTEGER, (random() * 11 + 1)::INTEGER, (random() * 27 + 1)::INTEGER) as FoundingDate,
                true as IsCurrent,
                1 as BatchID,
                DATE '2023-01-01' as EffectiveDate,
                DATE '9999-12-31' as EndDate
            FROM
                generate_series(1, {num_companies}) s(generate_series)
                CROSS JOIN (SELECT status FROM lookup_statuses ORDER BY random() LIMIT 1) st
                CROSS JOIN (SELECT name FROM lookup_company_names ORDER BY random() LIMIT 1) cn
                CROSS JOIN (SELECT industry FROM lookup_industries ORDER BY random() LIMIT 1) ind
                CROSS JOIN (SELECT rating FROM lookup_sp_ratings ORDER BY random() LIMIT 1) rating
                CROSS JOIN (SELECT state FROM lookup_us_states ORDER BY random() LIMIT 1) state
        """)

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += num_companies
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimCompany"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {num_companies:,} company records in {generation_time:.3f}s")

        return num_companies

    def generate_security_dimension(self, conn: Any) -> int:
        """Generate security dimension using SQL."""
        num_securities = int(self.base_securities * self.scale_factor)

        if self.enable_progress:
            self.logger.info(f"Generating {num_securities:,} security records...")

        start_time = time.time()

        conn.execute(f"""
            INSERT INTO DimSecurity
            SELECT
                s as SK_SecurityID,
                'SYM' || lpad(CAST(s AS VARCHAR), 4, '0') as Symbol,
                'S' as Issue,  -- Stock
                st.status as Status,
                'Security ' || lpad(CAST(s AS VARCHAR), 4, '0') as Name,
                exchange.exchange_id as ExchangeID,
                (random() * (SELECT MAX(SK_CompanyID) FROM DimCompany) + 1)::INTEGER as SK_CompanyID,
                (random() * (1000000000 - 1000000) + 1000000)::INTEGER as SharesOutstanding,
                make_date((random() * (2020 - 2000) + 2000)::INTEGER, (random() * (12 - 1) + 1)::INTEGER, (random() * (28 - 1) + 1)::INTEGER) as FirstTrade,
                make_date((random() * (2020 - 2000) + 2000)::INTEGER, (random() * (12 - 1) + 1)::INTEGER, (random() * (28 - 1) + 1)::INTEGER) as FirstTradeOnExchange,
                round((random() * 5.0), 2) as Dividend,
                true as IsCurrent,
                1 as BatchID,
                DATE '2023-01-01' as EffectiveDate,
                DATE '9999-12-31' as EndDate
            FROM
                generate_series(1, {num_securities}) s
                CROSS JOIN (SELECT status FROM lookup_statuses ORDER BY random() LIMIT 1) st
                CROSS JOIN (SELECT unnest(['NYSE', 'NASDAQ', 'AMEX']) as exchange_id ORDER BY random() LIMIT 1) exchange
        """)

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += num_securities
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimSecurity"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {num_securities:,} security records in {generation_time:.3f}s")

        return num_securities

    def generate_customer_dimension(self, conn: Any) -> int:
        """Generate customer dimension using SQL."""
        num_customers = int(self.base_customers * self.scale_factor)

        if self.enable_progress:
            self.logger.info(f"Generating {num_customers:,} customer records...")

        start_time = time.time()

        conn.execute(f"""
            INSERT INTO DimCustomer
            SELECT
                s as SK_CustomerID,
                s as CustomerID,
                'TAX' || lpad(CAST(s AS VARCHAR), 6, '0') as TaxID,
                st.status as Status,
                ln.name as LastName,
                fn.name as FirstName,
                chr((random() * (90 - 65) + 65)::INTEGER) as MiddleInitial,  -- Random A-Z
                CASE WHEN random() < 0.5 THEN 'M' ELSE 'F' END as Gender,
                (random() * (3 - 1) + 1)::INTEGER as Tier,
                make_date((random() * (2000 - 1950) + 1950)::INTEGER, (random() * (12 - 1) + 1)::INTEGER, (random() * (28 - 1) + 1)::INTEGER) as DOB,
                CAST((random() * (9999 - 100) + 100)::INTEGER AS INTEGER) || ' Customer St' as AddressLine1,
                CASE WHEN random() < 0.3 THEN 'Apt ' || CAST((random() * (999 - 1) + 1)::INTEGER AS INTEGER) ELSE NULL END as AddressLine2,
                lpad(CAST((random() * (99999 - 10000) + 10000)::INTEGER AS INTEGER), 5, '0') as PostalCode,
                'City' || CAST((s::INTEGER % 500) AS VARCHAR) as City,
                state.state as StateProv,
                'USA' as Country,
                '555-' || lpad(CAST((random() * (999 - 100) + 100)::INTEGER AS INTEGER), 3, '0') || '-' ||
                lpad(CAST((random() * (9999 - 1000) + 1000)::INTEGER AS INTEGER), 4, '0') as Phone1,
                CASE WHEN random() < 0.2 THEN
                    '555-' || lpad(CAST((random() * (999 - 100) + 100)::INTEGER AS INTEGER), 3, '0') || '-' ||
                    lpad(CAST((random() * (9999 - 1000) + 1000)::INTEGER AS INTEGER), 4, '0')
                ELSE NULL END as Phone2,
                NULL as Phone3,
                'customer' || s || '@example.com' as Email1,
                CASE WHEN random() < 0.1 THEN 'customer' || s || '.alt@example.com' ELSE NULL END as Email2,
                'Federal Tax' as NationalTaxRateDesc,
                0.25 as NationalTaxRate,
                'State Tax' as LocalTaxRateDesc,
                0.08 as LocalTaxRate,
                'AGENCY' || lpad(CAST((random() * (10 - 1) + 1)::INTEGER AS INTEGER), 2, '0') as AgencyID,
                (random() * (850 - 300) + 300)::INTEGER as CreditRating,
                (random() * (10000000 - 10000) + 10000)::INTEGER as NetWorth,
                'Customer Segment ' || (random() * (5 - 1) + 1)::INTEGER as MarketingNameplate,
                true as IsCurrent,
                1 as BatchID,
                DATE '2023-01-01' as EffectiveDate,
                DATE '9999-12-31' as EndDate
            FROM
                generate_series(1, {num_customers}) s
                CROSS JOIN (SELECT status FROM lookup_statuses ORDER BY random() LIMIT 1) st
                CROSS JOIN (SELECT name FROM lookup_first_names ORDER BY random() LIMIT 1) fn
                CROSS JOIN (SELECT name FROM lookup_last_names ORDER BY random() LIMIT 1) ln
                CROSS JOIN (SELECT state FROM lookup_us_states ORDER BY random() LIMIT 1) state
        """)

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += num_customers
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimCustomer"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {num_customers:,} customer records in {generation_time:.3f}s")

        return num_customers

    def generate_account_dimension(self, conn: Any) -> int:
        """Generate account dimension using SQL."""
        num_customers = int(self.base_customers * self.scale_factor)
        num_accounts = int(num_customers * 1.5)  # 1.5 accounts per customer average

        if self.enable_progress:
            self.logger.info(f"Generating {num_accounts:,} account records...")

        start_time = time.time()

        conn.execute(f"""
            INSERT INTO DimAccount
            SELECT
                s as SK_AccountID,
                s as AccountID,
                (random() * (100 - 1) + 1)::INTEGER as SK_BrokerID,  -- Assume 100 brokers
                (random() * (SELECT MAX(SK_CustomerID) FROM DimCustomer) + 1)::INTEGER as SK_CustomerID,
                st.status as Status,
                'Account ' || lpad(CAST(s AS VARCHAR), 6, '0') as AccountDesc,
                (random() * (2 - 0) + 0)::INTEGER as TaxStatus,  -- 0=Taxable, 1=Tax Deferred, 2=Tax Free
                true as IsCurrent,
                1 as BatchID,
                DATE '2023-01-01' as EffectiveDate,
                DATE '9999-12-31' as EndDate
            FROM
                generate_series(1, {num_accounts}) s
                CROSS JOIN (SELECT status FROM lookup_statuses ORDER BY random() LIMIT 1) st
        """)

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += num_accounts
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["DimAccount"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {num_accounts:,} account records in {generation_time:.3f}s")

        return num_accounts

    def generate_trade_facts(self, conn: Any) -> int:
        """Generate trade fact table using SQL."""
        num_trades = int(self.base_trades * self.scale_factor)

        if self.enable_progress:
            self.logger.info(f"Generating {num_trades:,} trade records...")

        start_time = time.time()

        conn.execute(f"""
            INSERT INTO FactTrade
            SELECT
                s as TradeID,
                (random() * (100 - 1) + 1)::INTEGER as SK_BrokerID,
                (random() * (SELECT MAX(SK_DateID) FROM DimDate) + 1)::INTEGER as SK_CreateDateID,
                (random() * (SELECT MAX(SK_TimeID) FROM DimTime) + 1)::INTEGER as SK_CreateTimeID,
                CASE WHEN random() < 0.8 THEN
                    (random() * (SELECT MAX(SK_DateID) FROM DimDate) + 1)::INTEGER
                ELSE NULL END as SK_CloseDateID,
                CASE WHEN random() < 0.8 THEN
                    (random() * (SELECT MAX(SK_TimeID) FROM DimTime) + 1)::INTEGER
                ELSE NULL END as SK_CloseTimeID,
                CASE
                    WHEN random() < 0.8 THEN 'Completed'
                    WHEN random() < 0.9 THEN 'Pending'
                    ELSE 'Cancelled'
                END as Status,
                tt.type as Type,
                CASE WHEN random() < 0.5 THEN true ELSE false END as CashFlag,
                (random() * (SELECT MAX(SK_SecurityID) FROM DimSecurity) + 1)::INTEGER as SK_SecurityID,
                (random() * (SELECT MAX(SK_CompanyID) FROM DimCompany) + 1)::INTEGER as SK_CompanyID,
                (random() * (10000 - 1) + 1)::INTEGER as Quantity,
                round((random() * 490.0 + 10.0), 2) as BidPrice,
                (random() * (SELECT MAX(SK_CustomerID) FROM DimCustomer) + 1)::INTEGER as SK_CustomerID,
                (random() * (SELECT MAX(SK_AccountID) FROM DimAccount) + 1)::INTEGER as SK_AccountID,
                'Broker' || (random() * (100 - 1) + 1)::INTEGER as ExecutedBy,
                round((random() * 490.0 + 10.0) * (random() * 0.04 + 0.98), 2) as TradePrice,
                round((random() * 45.0 + 5.0), 2) as Fee,
                round((random() * 490.0 + 10.0) * (random() * (10000 - 1) + 1)::INTEGER * 0.001, 2) as Commission,
                CASE WHEN random() < 0.8 THEN
                    round((random() * 490.0 + 10.0) * (random() * (10000 - 1) + 1)::INTEGER * 0.01, 2)
                ELSE 0.0 END as Tax,
                1 as BatchID
            FROM
                generate_series(1, {num_trades}) s
                CROSS JOIN (SELECT type FROM lookup_trade_types ORDER BY random() LIMIT 1) tt
        """)

        generation_time = time.time() - start_time
        self.generation_stats["records_generated"] += num_trades
        self.generation_stats["tables_generated"] += 1
        self.generation_stats["generation_times"]["FactTrade"] = generation_time
        self.generation_stats["sql_operations"] += 1

        if self.enable_progress:
            self.logger.info(f"✅ Generated {num_trades:,} trade records in {generation_time:.3f}s")

        return num_trades

    def generate_all_tables(self, conn: Any, tables: list[str] | None = None) -> dict[str, int]:
        """Generate all TPC-DI tables using SQL."""
        if tables is None:
            tables = [
                "DimDate",
                "DimTime",
                "DimCompany",
                "DimSecurity",
                "DimCustomer",
                "DimAccount",
                "FactTrade",
            ]

        if self.enable_progress:
            self.logger.info(f"Starting SQL-based TPC-DI data generation (Scale Factor: {self.scale_factor})")

        start_time = time.time()

        # Setup lookup tables first
        self._setup_lookup_tables(conn)

        results = {}

        # Generate tables in dependency order
        table_generators = {
            "DimDate": self.generate_date_dimension,
            "DimTime": self.generate_time_dimension,
            "DimCompany": self.generate_company_dimension,
            "DimSecurity": self.generate_security_dimension,
            "DimCustomer": self.generate_customer_dimension,
            "DimAccount": self.generate_account_dimension,
            "FactTrade": self.generate_trade_facts,
        }

        for table_name in tables:
            if table_name in table_generators:
                table_start = time.time()
                records_generated = table_generators[table_name](conn)
                results[table_name] = records_generated

                table_time = time.time() - table_start
                if self.enable_progress:
                    throughput = records_generated / table_time if table_time > 0 else 0
                    self.logger.info(f"  {table_name}: {throughput:,.0f} records/second")

        total_time = time.time() - start_time
        total_records = sum(results.values())

        if self.enable_progress:
            overall_throughput = total_records / total_time if total_time > 0 else 0
            self.logger.info(
                f"SQL generation completed: {total_records:,} records in {total_time:.3f}s ({overall_throughput:,.0f} records/sec)"
            )

        return results

    def get_generation_stats(self) -> dict[str, Any]:
        """Get generation statistics."""
        return {
            **self.generation_stats,
            "scale_factor": self.scale_factor,
            "estimated_records": {
                "DimCustomer": int(self.base_customers * self.scale_factor),
                "DimAccount": int(self.base_accounts * self.scale_factor),
                "DimSecurity": int(self.base_securities * self.scale_factor),
                "DimCompany": int(self.base_companies * self.scale_factor),
                "FactTrade": int(self.base_trades * self.scale_factor),
            },
        }

    def __del__(self):
        """Cleanup temporary connection on destruction."""
        self._close_temp_connection()


__all__ = ["TPCDISQLGenerator"]
