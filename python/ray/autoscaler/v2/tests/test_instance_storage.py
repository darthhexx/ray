# coding: utf-8
import os
import sys

import pytest  # noqa
from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.core.generated.instance_manager_pb2 import Instance


class DummySubscriber(InstanceUpdatedSuscriber):
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(events)


def create_instance(instance_id, status=Instance.INSTANCE_STATUS_UNSPECIFIED):
    return Instance(instance_id=instance_id, status=status)


def test_upsert_storage():
    subscriber = DummySubscriber()
    import pdb

    pdb.set_trace()
    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        status_change_subscriber=subscriber,
    )
    assert (True, 1) == storage.upsert_instances(
        [create_instance("instance1"), create_instance("instance2")],
        expected_storage_version=None,
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
