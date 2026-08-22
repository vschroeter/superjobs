from dataclasses import FrozenInstanceError

import pytest
from superjobs.jobs.job_identity import JobIdentity


@pytest.mark.parametrize(
    ("qualified_name", "expected_namespace", "expected_name", "expected_version"),
    [
        ("generate", None, "generate", None),
        ("generate:stable", None, "generate", "stable"),
        ("superjobs.generate", "superjobs", "generate", None),
        ("superjobs.generate:stable", "superjobs", "generate", "stable"),
        ("superjobs.jobs.generate:v2", "superjobs", "jobs.generate", "v2"),
    ],
)
def test_default_split_namespace_name_version(
    qualified_name: str,
    expected_namespace: str | None,
    expected_name: str,
    expected_version: str | None,
) -> None:
    assert JobIdentity.default_split_namespace_name_version(qualified_name) == (
        expected_namespace,
        expected_name,
        expected_version,
    )


def test_unqualified_identity_has_no_version() -> None:
    identity = JobIdentity(name="generate")

    assert identity.name == "generate"
    assert identity.namespace is None
    assert identity.version is None
    assert identity.canonical_name == "generate"


def test_qualified_name_is_split_into_namespace_name_and_version() -> None:
    identity = JobIdentity(name="superjobs.generate:stable")

    assert identity.namespace == "superjobs"
    assert identity.name == "generate"
    assert identity.version == "stable"
    assert identity.canonical_name == "superjobs.generate:stable"


@pytest.mark.parametrize(
    ("version", "expected_canonical_name"),
    [
        ("0", "superjobs.generate:0"),
        ("3", "superjobs.generate:3"),
    ],
)
def test_canonical_name_with_explicit_namespace(
    version: str,
    expected_canonical_name: str,
) -> None:
    identity = JobIdentity(
        name="generate",
        namespace="superjobs",
        version=version,
    )

    assert identity.canonical_name == expected_canonical_name


def test_identities_with_the_same_values_are_equal_and_hash_equal() -> None:
    first = JobIdentity(name="generate", namespace="superjobs", version="1")
    second = JobIdentity(name="generate", namespace="superjobs", version="1")

    assert first == second
    assert hash(first) == hash(second)
    assert {first, second} == {first}


def test_identity_is_immutable() -> None:
    identity = JobIdentity(name="generate", namespace="superjobs", version="1")

    with pytest.raises(FrozenInstanceError):
        identity.name = "other"
