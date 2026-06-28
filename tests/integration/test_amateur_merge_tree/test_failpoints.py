#!/usr/bin/env python3

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


def test_insert_zk_fail_after_part_renamed(started_cluster):
    """
    ZK failure during part commit after part rename
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_insert_zk_fail_after_part_renamed (key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_insert_zk_fail_after_part_renamed', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    cluster.instances["node1"].query("""
        SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op
    """)

    cluster.instances["node1"].query("""
        INSERT INTO test_insert_zk_fail_after_part_renamed SELECT number, 'val-' || number FROM numbers(10)
    """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_insert_zk_fail_after_part_renamed")

    expected = [[10, 0, 'val-0', 9, 'val-9']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_insert_zk_fail_after_part_renamed
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"

def test_insert_unknown_status(started_cluster):
    """
    ZK failure during part commit after part rename, which cannot be recovered
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_insert_unknown_status(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_insert_unknown_status', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    failpoints = [
        "replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault",
        "replicated_merge_tree_commit_zk_fail_after_op",
    ]

    cluster.instances["node1"].query("""
        INSERT INTO test_insert_unknown_status SELECT number, 'val-' || number FROM numbers(10)
    """)

    for fp in failpoints:
        cluster.instances["node1"].query(f"SYSTEM ENABLE FAILPOINT {fp}")

    cluster.instances["node1"].query("""
        INSERT INTO test_insert_unknown_status SELECT number + 10, 'val-' || number + 10 FROM numbers(10)
        SETTINGS insert_keeper_max_retries = 2 -- {serverError UNKNOWN_STATUS_OF_INSERT}
    """)

    for fp in failpoints:
        cluster.instances["node1"].query(f"SYSTEM DISABLE FAILPOINT {fp}")

    cluster.instances["node1"].query("""
        INSERT INTO test_insert_unknown_status SELECT number + 20, 'val-' || number + 20 FROM numbers(10)
    """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_insert_unknown_status")

    expected = [[30, 0, 'val-0', 29, 'val-29']]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key) FROM test_insert_unknown_status
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"


def test_merge_fail_before_commit(started_cluster):
    """
    ZK failure before part commit during merge
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_merge_fail_before_commit(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_merge_fail_before_commit', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(2):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_merge_fail_before_commit
            SELECT number + {i * 10}, 'val-' || {i * 10} + number
            FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_merge_fail_before_commit")

    cluster.instances["node1"].query("SYSTEM ENABLE FAILPOINT amateur_merge_commit_zk_fail_before_op")

    merge_exception = None
    try:
        cluster.instances["node1"].query("OPTIMIZE TABLE test_merge_fail_before_commit")
    except Exception as ex:
        merge_exception = ex

    assert merge_exception is not None
    assert "Coordination::Exception: Fault injection before operation" in merge_exception.stderr

    expected = [[20, 0, 'val-0', 19, 'val-19', 2]]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key), uniqExact(_part) FROM test_merge_fail_before_commit
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"


def test_merge_fail_after_commit(started_cluster):
    """
    ZK failure after part commit during merge
    """

    for node_name, node in cluster.instances.items():
        data = node.query(f"""
            CREATE TABLE test_merge_fail_after_commit(key UInt32, val String)
            ENGINE = AmateurMergeTree('/clickhouse/tables/default/test_merge_fail_after_commit', '{node_name}')
            ORDER BY key
            SETTINGS storage_policy = 's3'
        """)

    for i in range(2):
        cluster.instances["node1"].query(f"""
            INSERT INTO test_merge_fail_after_commit
            SELECT number + {i * 10}, 'val-' || {i * 10} + number
            FROM numbers(10)
        """)

    zk_client = cluster.get_kazoo_client("zoo1")
    wait_table_sync(zk_client, "/clickhouse/tables/default/test_merge_fail_after_commit")

    cluster.instances["node1"].query("SYSTEM ENABLE FAILPOINT amateur_merge_commit_zk_fail_after_op")

    merge_exception = None
    try:
        cluster.instances["node1"].query("OPTIMIZE TABLE test_merge_fail_after_commit")
    except Exception as ex:
        merge_exception = ex

    assert merge_exception is not None
    assert "Coordination::Exception: Fault injection after operation" in merge_exception.stderr

    wait_table_sync(zk_client, "/clickhouse/tables/default/test_merge_fail_after_commit")

    expected = [[20, 0, 'val-0', 19, 'val-19', 1]]
    for node_name, node in cluster.instances.items():
        actual = node.query("""
            SELECT count(), min(key), argMin(val, key), max(key), argMax(val, key), uniqExact(_part) FROM test_merge_fail_after_commit
        """)

        assert TSV(actual) == TSV(expected), f"Received an unexpected result from node {node_name}"
