import time


def wait_table_sync(zk_client, zk_table_path):
    _, parts_stat = zk_client.get(f"{zk_table_path}/parts")
    nodes = zk_client.get_children(f"{zk_table_path}/replicas")

    for _ in range(60):
        nodes_in_sync = 0
        for node in nodes:
            node_epoch, _ = zk_client.get(f"{zk_table_path}/replicas/{node}/epoch")
            if int(node_epoch) >= parts_stat.pzxid:
                nodes_in_sync += 1

        if nodes_in_sync == len(nodes):
            return

        time.sleep(0.5)
