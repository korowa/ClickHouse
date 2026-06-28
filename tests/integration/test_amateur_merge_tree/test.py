#!/usr/bin/env python3

import concurrent.futures
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

from test_amateur_merge_tree.helpers import wait_table_sync

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.yaml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
            use_keeper=False,
            cpu_limit=3,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.yaml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
            use_keeper=False,
            cpu_limit=3
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_insert_select(started_cluster):
    """
    Basic scenario with inserts and replica synchronization
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_basic (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_basic', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        node = list(cluster.instances.values())[i % 2]
        node.query(f"""
            INSERT INTO test_basic SELECT number + {i} * 10 as key, 'val-' || key AS val FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_basic")

    expected = [[100, 0, 'val-0', 99, 'val-99']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_basic
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"


def test_drop_partition(started_cluster):
    """
    Insert into partitioned tables and drop partition
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_partitioned (key UInt32, prt UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_partitioned', '{node_name}')
            PARTITION BY prt
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        node = list(cluster.instances.values())[i % 2]
        node.query(f"""
            INSERT INTO test_partitioned SELECT number + {i} * 10 as key, number % 5 AS prt, 'val-' || key FROM numbers(10)
        """)

    cluster.instances["node1"].query("ALTER TABLE test_partitioned DROP PARTITION 1")
    cluster.instances["node2"].query("ALTER TABLE test_partitioned DROP PARTITION 3")

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_partitioned")

    expected = [[60, 0, 'val-0', 99, 'val-99', 0, 0]]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key),
                   countIf(prt = 1), countIf(prt = 3)
            FROM test_partitioned
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"


def test_drop_partition_while_reading(started_cluster):
    """
    Insert into partitioned tables, run loop query on one node, and drop partitions on another
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_drop_partition_while_reading(key UInt32, prt UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_drop_partition_while_reading', '{node_name}')
            PARTITION BY prt
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_drop_partition_while_reading SELECT number + {i} * 10 as key, number % 5 AS prt, 'val-' || key FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_drop_partition_while_reading")

    response = cluster.instances["node2"].get_query_request(
        f"SELECT * FROM loop(test_drop_partition_while_reading) settings max_threads = 1",
        query_id="test_drop_partition_loop"
    )

    for attempt in range(10):
        try:
            cluster.instances["node1"].query("ALTER TABLE test_drop_partition_while_reading DROP PARTITION 1")
            cluster.instances["node1"].query("ALTER TABLE test_drop_partition_while_reading DROP PARTITION 3")
        except Exception as ex:
            if "Transaction failed (No node)" in str(ex.stderr):
                if attempt == 9:
                    raise
                time.sleep(1)
                continue
            else:
                raise ex

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_drop_partition_while_reading")

    proc = cluster.instances["node2"].query("SELECT count() FROM system.processes WHERE query_id = 'test_drop_partition_loop'")
    assert int(proc.strip()) == 1, f"Unexpected qty of loop queries running: {proc.strip()}"

    expected = [[60, 0, 'val-0', 99, 'val-99', 0, 0]]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key),
                   countIf(prt = 1), countIf(prt = 3)
            FROM test_drop_partition_while_reading
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"


def test_optimize(started_cluster):
    """
    Run optimize on both replicas
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_optimize(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_optimize', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        node = list(cluster.instances.values())[i % 2]
        node.query(f"""
            INSERT INTO test_optimize SELECT number + {i} * 10 as key, 'val-' || key FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_optimize")

    for node_name, node in cluster.instances.items():
        actual = node.query("OPTIMIZE TABLE test_optimize FINAL")

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_optimize")

    expected = [[60, 0, 'val-0', 99, 'val-99', 0, 0, 'all_0_9']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key),
                   any(substr(_part, 1, 7))
            FROM test_optimize
        """)


def test_concurrent_optimize(started_cluster):
    """
    First completed optimzie is winner, latter won't introduce incorrect data
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_concurrent_optimize (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_concurrent_optimize', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_concurrent_optimize
            SELECT number + {i} * 10 as key, 'val-' || key AS val FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_concurrent_optimize")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(cluster.instances["node1"].query, "OPTIMIZE TABLE test_concurrent_optimize FINAL"),
            executor.submit(cluster.instances["node2"].query, "OPTIMIZE TABLE test_concurrent_optimize FINAL"),
        ]
        concurrent.futures.wait(futures)

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_concurrent_optimize")

    expected = [[100, 0, 'val-0', 99, 'val-99', 'all_0_9', 'all_0_9']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key),
                   min(substr(_part, 1, 7)), max(substr(_part, 1, 7))
            FROM test_concurrent_optimize
        """)
        assert TSV(actual) == TSV(expected), f"Unexpected result from {node_name}"


def test_background_merge(started_cluster):
    """
    Same as testing optimize, but waiting for background merges
    """

    for node_name, node in cluster.instances.items():
        node.query(f"""
            CREATE TABLE test_background_merge (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_background_merge', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(50):
        node = list(cluster.instances.values())[i % 2]
        cluster.instances["node1"].query(f"""
            INSERT INTO test_background_merge
            SELECT number + {i} * 10 as key, 'val-' || toString(number + {i} * 10) AS val
            FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_background_merge")

    for attempt in range(60):
        parts = cluster.instances["node1"].query("""
            SELECT count() FROM system.parts
            WHERE table = 'test_background_merge' AND active = 1
        """).strip()
        if int(parts) < 50:
            break
        time.sleep(1)

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_background_merge")

    for node_name, node in cluster.instances.items():
        parts = cluster.instances["node1"].query("""
            SELECT count() FROM system.parts
            WHERE table = 'test_background_merge' AND active = 1
        """).strip()
        assert int(parts) < 50, f"Background merges did not run, still {parts} parts"

    expected = [[500, 0, 'val-0', 499, 'val-499']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key)
            FROM test_background_merge
        """)
        assert TSV(actual) == TSV(expected), f"Unexpected result from {node_name}"


def test_cleanup(started_cluster):
    """
    Ensure cleanup won't affect running queries
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_cleanup(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_cleanup', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3', cleanup_delay_period = 1, old_parts_lifetime = 1
        """)

    for i in range(10):
        node = list(cluster.instances.values())[i % 2]
        node.query(f"""
            INSERT INTO test_cleanup SELECT number + {i} * 10 as key, 'val-' || key FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_cleanup")

    response = cluster.instances["node2"].get_query_request(
        f"SELECT * FROM loop(test_cleanup) settings max_threads = 1",
        query_id="test_cleanup_loop"
    )
    cluster.instances["node1"].query("OPTIMIZE TABLE test_cleanup FINAL")

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_cleanup")

    expected = [[60, 0, 'val-0', 99, 'val-99', 0, 0, 'all_0_9']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key),
                   any(substr(_part, 1, 7))
            FROM test_cleanup
        """)

    parts = cluster.instances["node1"].query("""
        SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_cleanup/parts'
    """).strip()
    parts_stale = cluster.instances["node1"].query("""
        SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_cleanup/parts_stale'
    """).strip()
    loop_query = cluster.instances["node2"].query("""
        SELECT count() FROM system.processes WHERE query_id = 'test_cleanup_loop'
    """)

    assert int(parts) == 1, "Expected to have a single active part"
    assert int(parts_stale) > 0, "Expected non-zero amount of stale parts"
    assert int(loop_query) == 1, "Expected loop query running after optimize"

    cluster.instances["node2"].query("KILL QUERY WHERE query_id = 'test_cleanup_loop' SYNC")

    for attempt in range(10):
        parts_stale = cluster.instances["node1"].query("""
            SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_cleanup/parts_stale'
        """).strip()
        if int(parts_stale) > 0:
            if attempt == 9:
                raise RuntimeError("Failed to wait for stale parts cleanup")
            time.sleep(1)


def test_add_remove_replica(started_cluster):
    """
    Add table replica while the data already exists
    """

    cluster.instances["node1"].query("""
        CREATE TABLE test_add_remove_replica (key UInt32, val String)
        ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_add_remove_replica', 'node1')
        ORDER BY key
        SETTINGS storage_policy = 's3'
    """)

    for i in range(10):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_add_remove_replica SELECT number + {i} * 10 as key, 'val-' || key AS val FROM numbers(10)
        """)

    expected = [[100, 0, 'val-0', 99, 'val-99']]

    actual_first = cluster.instances["node1"].query("""
        SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_add_remove_replica
    """)
    assert TSV(actual_first) == TSV(expected), "Received an unexpected result from node1"

    cluster.instances["node2"].query("""
        CREATE TABLE test_add_remove_replica (key UInt32, val String)
        ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_add_remove_replica', 'node2')
        ORDER BY key
        SETTINGS storage_policy = 's3'
    """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_add_remove_replica")

    actual_replica = cluster.instances["node2"].query("""
        SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_add_remove_replica
    """)
    assert TSV(actual_first) == TSV(expected), "Received an unexpected result from node2"

    cluster.instances["node2"].query("DROP TABLE test_add_remove_replica SYNC")

    actual_first = cluster.instances["node1"].query("""
        SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_add_remove_replica
    """)
    assert TSV(actual_first) == TSV(expected), "Received an unexpected result from node1 after replica drop"


def test_drop_first_replica(started_cluster):
    """
    Assert second replica data will be available after first replica dropped
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_drop_first_replica(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_drop_first_replica', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(10):
        node = cluster.instances["node1"]
        node.query(f"""
            INSERT INTO test_drop_first_replica SELECT number + {i} * 10 as key, 'val-' || key FROM numbers(10)
        """)

    cluster.instances["node1"].query("DROP TABLE test_drop_first_replica SYNC")

    node = cluster.instances["node2"]
    attempts = 0
    while True:
        rowcount = node.query("SELECT count() FROM test_drop_first_replica").strip()

        if int(rowcount) == 100:
            break

        if attempts >= 60:
            raise RuntimeError("Failed to wait for expected rowcount")

        attempts += 1
        time.sleep(0.5)

    expected = [[0, 'val-0', 99, 'val-99']]
    actual = node.query("""
        SELECT min(key), argMin(val, key), max(key), argMax(val, key)
        FROM test_drop_first_replica
    """)
    assert TSV(actual) == TSV(expected), "Received an unexpected result from node2 after replica drop"


def test_epoch_hold_by_snapshot(started_cluster):
    """
    Assert epoch (used for cleanup) not propagating when snapshot is hold by long running query
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_epoch_hold_by_snapshot (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_epoch_hold_by_snapshot', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3', old_parts_lifetime = 1
        """)

    cluster.instances["node1"].query(f"""
        INSERT INTO test_epoch_hold_by_snapshot SELECT number, 'val-' || number FROM numbers(10)
    """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_epoch_hold_by_snapshot")

    node1_initial_epoch = cluster.instances["node1"].query("""
        SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_epoch_hold_by_snapshot/replicas/node1'
    """).strip()
    node2_initial_epoch = cluster.instances["node1"].query("""
        SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_epoch_hold_by_snapshot/replicas/node2'
    """).strip()

    assert int(node1_initial_epoch) == int(node2_initial_epoch), "Expected initial epochs to be equal"

    response = cluster.instances["node2"].get_query_request(
        f"SELECT * FROM loop(test_epoch_hold_by_snapshot) settings max_threads = 1",
        query_id="hold_snapshot"
    )

    for _ in range(3):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_epoch_hold_by_snapshot SELECT number, 'val-' || number FROM numbers(10)
        """)

    cluster.instances["node1"].query("""
        OPTIMIZE TABLE test_epoch_hold_by_snapshot FINAL
        SETTINGS alter_sync = 1, optimize_throw_if_noop = 1
    """)

    time.sleep(2)

    node1_after_opt_epoch = cluster.instances["node1"].query("""
        SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_epoch_hold_by_snapshot/replicas/node1'
    """).strip()
    node2_after_opt_epoch = cluster.instances["node1"].query("""
        SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_epoch_hold_by_snapshot/replicas/node2'
    """).strip()

    assert int(node1_after_opt_epoch) > int(node2_after_opt_epoch), "Expected node1 epoch to be greater than node2 epoch"
    assert int(node2_initial_epoch) == int(node2_after_opt_epoch), "Expected node2 unchanged after inserts / optimize"

    response = cluster.instances["node2"].query("KILL QUERY WHERE query_id = 'hold_snapshot' SYNC")

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_epoch_hold_by_snapshot")

    node2_after_sync_epoch = cluster.instances["node1"].query("""
        SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/default/test_epoch_hold_by_snapshot/replicas/node2'
    """).strip()

    assert int(node1_after_opt_epoch) == int(node2_after_sync_epoch), "Expected epochs to be equal after final sync"


def test_deduplicate(started_cluster):
    """
    Assert block deduplication
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_deduplicate(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_deduplicate', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(6):
        node = list(cluster.instances.values())[i % 2]
        node.query(f"""
            INSERT INTO test_deduplicate VALUES (0, 'a'), (1, 'b'), (2, 'c'), (4, 'd'), (5, 'e')
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_deduplicate")

    expected = [[5, 0, 'a', 5, 'e']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key)
            FROM test_deduplicate
        """)


def test_replica_catch_up(started_cluster):
    """
    Shutdown one replica and verify it reloads its parts and catches up
    with inserts completed while it was stopped.
    """

    for node_name, node in cluster.instances.items():
        node.query(f"""
            CREATE TABLE test_replica_catch_up (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_replica_catch_up', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    cluster.instances["node1"].query(f"""
        INSERT INTO test_replica_catch_up SELECT number, 'val-' || number FROM numbers(50)
    """)
    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_replica_catch_up")

    cluster.instances["node2"].stop_clickhouse()

    for i in range(5):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_replica_catch_up
            SELECT number + 50 + {i} * 10 as key, 'val-' || key AS val FROM numbers(10)
        """)

    cluster.instances["node2"].start_clickhouse()

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_replica_catch_up")

    expected = [[100, 0, 'val-0', 99, 'val-99']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key)
            FROM test_replica_catch_up
        """)
        assert TSV(actual) == TSV(expected), f"Unexpected result from {node_name}"


def test_async_insert(started_cluster):
    """
    Test async insert deduplication and retry behavior.
    Async inserts use a different deduplication mechanism (block_ids) than sync inserts.
    """

    for node_name, node in cluster.instances.items():
        node.query(f"""
            CREATE TABLE test_async_insert (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_async_insert', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    cluster.instances["node1"].query(
        "INSERT INTO test_async_insert VALUES (1, 'a'), (2, 'b')",
        settings={"async_insert": 1, "wait_for_async_insert": 1}
    )

    cluster.instances["node2"].query(
        "INSERT INTO test_async_insert VALUES (1, 'a'), (2, 'b')",
        settings={"async_insert": 1, "wait_for_async_insert": 1}
    )

    cluster.instances["node1"].query(
        "INSERT INTO test_async_insert VALUES (3, 'c'), (4, 'd')",
        settings={"async_insert": 1, "wait_for_async_insert": 1}
    )

    cluster.instances["node2"].query(
        "INSERT INTO test_async_insert VALUES (5, 'e')",
        settings={"async_insert": 1, "wait_for_async_insert": 1}
    )

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_async_insert")

    expected = [[5, 1, 'a', 5, 'e']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key)
            FROM test_async_insert
        """)
        assert TSV(actual) == TSV(expected), f"Unexpected result from {node_name}: {actual}"
