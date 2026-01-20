Amazon Redshift Platform Adapter
==================================

.. tags:: reference, python-api, cloud-platform

The Redshift adapter provides AWS-native data warehouse execution with S3 integration and columnar storage optimization.

Overview
--------

Amazon Redshift is a fully managed petabyte-scale data warehouse service that provides:

- **Columnar storage** - Optimized for analytical queries
- **Massively parallel processing** - Distributed query execution
- **S3 integration** - COPY command for bulk loading
- **Automatic backups** - Point-in-time recovery capabilities
- **Concurrency scaling** - Automatic scaling for concurrent workloads
- **Redshift Spectrum** - Query data directly in S3

Common use cases:

- AWS-native analytics workloads
- Large-scale data warehousing (TB to PB scale)
- Integration with AWS ecosystem
- Reserved or serverless deployment options
- Federated queries across data lake and warehouse

Quick Start
-----------

Basic Configuration
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from benchbox.tpch import TPCH
    from benchbox.platforms.redshift import RedshiftAdapter

    # Connect to Redshift cluster
    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        port=5439,
        database="dev",
        username="admin",
        password="SecurePassword123"
    )

    # Run benchmark
    benchmark = TPCH(scale_factor=1.0)
    results = benchmark.run_with_platform(adapter)

With S3 Data Loading
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Efficient loading via S3 COPY command
    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="SecurePassword123",
        database="benchbox",
        s3_bucket="my-redshift-data",
        s3_prefix="benchbox/staging",
        iam_role="arn:aws:iam::123456789:role/RedshiftCopyRole"
    )

API Reference
-------------

RedshiftAdapter Class
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: benchbox.platforms.redshift.RedshiftAdapter
   :members:
   :undoc-members:
   :show-inheritance:

Constructor Parameters
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    RedshiftAdapter(
        host: str,
        username: str,
        password: str,
        port: int = 5439,
        database: str = "dev",
        cluster_identifier: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "benchbox-data",
        iam_role: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: str = "us-east-1",
        wlm_query_slot_count: int = 1,
        compression_encoding: str = "AUTO",
        auto_vacuum: bool = True,
        auto_analyze: bool = True
    )

Parameters:

**Connection**:
- **host** (str): Cluster endpoint hostname
- **username** (str): Database user
- **password** (str): User password
- **port** (int): Cluster port. Default: 5439
- **database** (str): Database name. Default: "dev"

**S3 Integration**:
- **s3_bucket** (str, optional): S3 bucket for data staging
- **s3_prefix** (str): Prefix in bucket. Default: "benchbox-data"
- **iam_role** (str, optional): IAM role ARN for COPY
- **aws_access_key_id** (str, optional): AWS access key
- **aws_secret_access_key** (str, optional): AWS secret key
- **aws_region** (str): AWS region. Default: "us-east-1"

**Optimization**:
- **wlm_query_slot_count** (int): WLM slots. Default: 1
- **compression_encoding** (str): Compression type. Default: "AUTO"
- **auto_vacuum** (bool): Auto vacuum tables. Default: True
- **auto_analyze** (bool): Auto analyze tables. Default: True

Configuration Examples
----------------------

IAM Role Authentication (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Create IAM role with S3 read permissions
    aws iam create-role --role-name RedshiftCopyRole \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }'

    # Attach S3 read policy
    aws iam attach-role-policy --role-name RedshiftCopyRole \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

    # Associate role with cluster
    Amazon Redshift modify-cluster-iam-roles \
        --cluster-identifier my-cluster \
        --add-iam-roles arn:aws:iam::123456789:role/RedshiftCopyRole

.. code-block:: python

    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="password",
        s3_bucket="my-data-bucket",
        iam_role="arn:aws:iam::123456789:role/RedshiftCopyRole"
    )

Access Key Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="password",
        s3_bucket="my-data-bucket",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        aws_region="us-east-1"
    )

Workload Management (WLM)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Use multiple query slots for large queries
    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="password",
        wlm_query_slot_count=3  # Use 3 slots for more resources
    )

Data Loading
------------

Via S3 COPY (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from benchbox.platforms.redshift import RedshiftAdapter
    from benchbox.tpch import TPCH
    from pathlib import Path

    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="password",
        database="benchbox",
        s3_bucket="my-redshift-staging",
        s3_prefix="benchbox/tpch",
        iam_role="arn:aws:iam::123456789:role/RedshiftCopyRole"
    )

    # Generate data locally
    benchmark = TPCH(scale_factor=1.0)
    data_dir = Path("./tpch_data")
    benchmark.generate_data(data_dir)

    # Load data (automatically uploads to S3 then uses COPY)
    conn = adapter.create_connection()
    table_stats, load_time = adapter.load_data(benchmark, conn, data_dir)

    print(f"Loaded {sum(table_stats.values()):,} rows in {load_time:.2f}s")

Direct Loading (Small Datasets)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # For small datasets (< 100MB), skip S3
    adapter = RedshiftAdapter(
        host="my-cluster.123456.us-east-1.redshift.amazonaws.com",
        username="admin",
        password="password"
        # No s3_bucket specified - uses direct INSERT
    )

Advanced Features
-----------------

Distribution Keys
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Create table with distribution key
    cursor.execute("""
        CREATE TABLE orders (
            o_orderkey BIGINT,
            o_custkey BIGINT,
            o_orderstatus CHAR(1),
            o_totalprice DECIMAL(15,2),
            o_orderdate DATE
        )
        DISTSTYLE KEY
        DISTKEY (o_custkey)
        SORTKEY (o_orderdate)
    """)

Sort Keys
~~~~~~~~~

.. code-block:: python

    # Compound sort key (most common)
    CREATE TABLE lineitem (...)
    SORTKEY (l_shipdate, l_orderkey)

    # Interleaved sort key (for multiple filters)
    CREATE TABLE lineitem (...)
    INTERLEAVED SORTKEY (l_shipdate, l_orderkey, l_partkey)

Compression
~~~~~~~~~~~

.. code-block:: python

    # Automatic compression analysis
    adapter = RedshiftAdapter(
        host="my-cluster...",
        compression_encoding="AUTO"  # Redshift analyzes and applies optimal encoding
    )

    # Check compression
    cursor.execute("""
        SELECT
            "column",
            type,
            encoding
        FROM pg_table_def
        WHERE tablename = 'lineitem'
    """)

Vacuum and Analyze
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Manual maintenance
    adapter.vacuum_table(conn, "lineitem")
    adapter.analyze_table(conn, "lineitem")

    # Or automatic
    adapter = RedshiftAdapter(
        auto_vacuum=True,
        auto_analyze=True
    )

Best Practices
--------------

Distribution Strategy
~~~~~~~~~~~~~~~~~~~~~

1. **Choose appropriate DISTSTYLE**:

   .. code-block:: sql

       -- EVEN: Small tables, no joins
       CREATE TABLE region (...) DISTSTYLE EVEN

       -- KEY: Large fact tables (distribute by join key)
       CREATE TABLE orders (...) DISTSTYLE KEY DISTKEY (o_custkey)

       -- ALL: Small dimension tables (broadcast to all nodes)
       CREATE TABLE nation (...) DISTSTYLE ALL

Sort Keys
~~~~~~~~~

1. **Use compound sort keys** for range/equality filters:

   .. code-block:: sql

       -- Good for: WHERE l_shipdate BETWEEN ... AND l_orderkey = ...
       SORTKEY (l_shipdate, l_orderkey)

2. **Use interleaved for multiple filter combinations**:

   .. code-block:: sql

       -- Good for varying filter combinations
       INTERLEAVED SORTKEY (l_shipdate, l_orderkey, l_partkey)

Data Loading
~~~~~~~~~~~~

1. **Use COPY from S3** for best performance
2. **Load compressed files** (GZIP recommended)
3. **Use manifest files** for multiple files
4. **Run ANALYZE after loading**

Cost Optimization
~~~~~~~~~~~~~~~~~

1. **Use reserved instances** for predictable workloads
2. **Pause clusters** when not in use
3. **Use concurrency scaling** for burst workloads
4. **Monitor query performance** with system tables

Common Issues
-------------

Connection Timeout
~~~~~~~~~~~~~~~~~~

**Problem**: Cannot connect to cluster

**Solutions**:

.. code-block:: bash

    # 1. Check cluster status
    Amazon Redshift describe-clusters --cluster-identifier my-cluster

    # 2. Verify security group allows inbound on port 5439
    # 3. Check VPC routing and NAT gateway

    # 4. Test connectivity
    psql -h my-cluster.123456.us-east-1.redshift.amazonaws.com \
         -U admin -d dev -p 5439

S3 COPY Errors
~~~~~~~~~~~~~~

**Problem**: COPY command fails

**Solutions**:

.. code-block:: python

    # 1. Verify IAM role permissions
    # Role needs: s3:GetObject, s3:ListBucket

    # 2. Check S3 bucket region matches cluster region

    # 3. View error details
    cursor.execute("""
        SELECT * FROM stl_load_errors
        ORDER BY starttime DESC
        LIMIT 10
    """)

Slow Query Performance
~~~~~~~~~~~~~~~~~~~~~~

**Problem**: Queries slower than expected

**Solutions**:

.. code-block:: python

    # 1. Check query execution plan
    plan = adapter.get_query_plan(conn, query)

    # 2. Verify distribution keys
    cursor.execute("""
        SELECT
            TRIM(t.name) AS table,
            TRIM(c.name) AS column,
            c.distkey
        FROM stv_tbl_perm t
        JOIN pg_attribute a ON a.attrelid = t.id
        JOIN pg_class c ON c.oid = t.id
        WHERE c.distkey = TRUE
    """)

    # 3. Check sort key usage
    cursor.execute("""
        SELECT * FROM svv_table_info
        WHERE "table" = 'lineitem'
    """)

    # 4. Run VACUUM and ANALYZE
    adapter.vacuum_table(conn, "lineitem")
    adapter.analyze_table(conn, "lineitem")

See Also
--------

Platform Documentation
~~~~~~~~~~~~~~~~~~~~~~

- :doc:`/platforms/platform-selection-guide` - Choosing Redshift vs other platforms
- :doc:`/platforms/quick-reference` - Quick setup for all platforms
- :doc:`/platforms/comparison-matrix` - Feature comparison

API Reference
~~~~~~~~~~~~~

- :doc:`snowflake` - Snowflake adapter
- :doc:`databricks` - Databricks adapter
- :doc:`bigquery` - BigQuery adapter
- :doc:`../base` - Base benchmark interface
- :doc:`../index` - Python API overview

External Resources
~~~~~~~~~~~~~~~~~~

- `Redshift Documentation <https://docs.aws.amazon.com/redshift/>`_ - Official Redshift docs
- `Best Practices <https://docs.aws.amazon.com/redshift/latest/dg/best-practices.html>`_ - Performance optimization
- `Distribution Styles <https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html>`_ - Distribution guidance
- `COPY Command <https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html>`_ - Data loading reference
