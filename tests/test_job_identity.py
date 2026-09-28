from dataclasses import FrozenInstanceError

import pytest

from superjobs.jobs.job_identity import JobIdentity


def test_hierarchical_identity_is_not_split_into_namespace_and_name() -> None:
    identity = JobIdentity("superjobs.jobs.generate", version="v2")

    assert identity.name == "superjobs.jobs.generate"
    assert identity.version == "v2"
    assert identity.canonical_name == "superjobs.jobs.generate:v2"


def test_unversioned_identity_remains_unversioned() -> None:
    identity = JobIdentity(name="generate")

    assert identity.version is None
    assert identity.canonical_name == "generate"


def test_embedded_version_is_rejected() -> None:
    with pytest.raises(ValueError):
        JobIdentity("superjobs.generate:v1")


def test_identities_with_the_same_values_are_equal_and_hash_equal() -> None:
    first = JobIdentity(name="superjobs.generate", version="1")
    second = JobIdentity(name="superjobs.generate", version="1")

    assert first == second
    assert hash(first) == hash(second)
    assert {first, second} == {first}


def test_identity_is_immutable() -> None:
    identity = JobIdentity(name="superjobs.generate", version="1")

    with pytest.raises(FrozenInstanceError):
        identity.name = "other"
