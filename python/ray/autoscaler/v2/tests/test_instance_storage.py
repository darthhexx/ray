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

    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        status_change_subscriber=subscriber,
    )
    instance1 = create_instance("instance1")
    instance2 = create_instance("instance2")
    instance3 = create_instance("instance3")

    assert (True, 1) == storage.upsert_instances(
        [instance1, instance2],
        expected_storage_version=None,
    )

    instance1.version = 1
    instance2.version = 1
    entries, storage_version = storage.get_instances()

    assert storage_version == 1
    assert entries == {
        "instance1": instance1,
        "instance2": instance2,
    }

    assert (False, 1) == storage.upsert_instances(
        [create_instance("instance1"), create_instance("instance2")],
        expected_storage_version=0,
    )

    instance2.status = Instance.IDLE
    assert (True, 2) == storage.upsert_instances(
        [instance3, instance2],
        expected_storage_version=1,
    )

    instance1.version = 1
    instance2.version = 2
    instance3.version = 2
    entries, storage_version = storage.get_instances()

    assert storage_version == 2
    assert entries == {
        "instance1": instance1,
        "instance2": instance2,
        "instance3": instance3,
    }


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
