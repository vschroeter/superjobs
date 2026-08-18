from dataclasses import FrozenInstanceError

import pytest

from superjobs.jobs.job import JobIdentity


@pytest.mark.parametrize(
    ("qualified_name", "expected_namespace", "expected_name"),
    [
        ("generate", None, "generate"),
        ("superjobs.generate", "superjobs", "generate"),
        ("superjobs.jobs.generate", "superjobs", "jobs.generate"),
    ],
)
def test_default_split_namespace_and_name(
    qualified_name: str,
    expected_namespace: str | None,
    expected_name: str,
) -> None:
    assert JobIdentity.default_split_namespace_and_name(qualified_name) == (
        expected_namespace,
        expected_name,
    )


def test_unqualified_identity_uses_default_version() -> None:
    identity = JobIdentity(name="generate")

    assert identity.name == "generate"
    assert identity.namespace is None
    assert identity.version == 0
    assert identity.canonical_name == "generate:0"


def test_qualified_name_is_split_into_namespace_and_name() -> None:
    identity = JobIdentity(name="superjobs.generate", version=1)

    assert identity.namespace == "superjobs"
    assert identity.name == "generate"
    assert identity.version == 1
    assert identity.canonical_name == "superjobs.generate:v1"


@pytest.mark.parametrize(
    ("version", "expected_canonical_name"),
    [
        (0, "superjobs.generate:v0"),
        (3, "superjobs.generate:v3"),
    ],
)
def test_canonical_name_with_explicit_namespace(
    version: int,
    expected_canonical_name: str,
) -> None:
    identity = JobIdentity(
        name="generate",
        namespace="superjobs",
        version=version,
    )

    assert identity.canonical_name == expected_canonical_name


def test_identities_with_the_same_values_are_equal_and_hash_equal() -> None:
    first = JobIdentity(name="generate", namespace="superjobs", version=1)
    second = JobIdentity(name="generate", namespace="superjobs", version=1)

    assert first == second
    assert hash(first) == hash(second)
    assert {first, second} == {first}


def test_identity_is_immutable() -> None:
    identity = JobIdentity(name="generate", namespace="superjobs", version=1)

    with pytest.raises(FrozenInstanceError):
        identity.name = "other"
