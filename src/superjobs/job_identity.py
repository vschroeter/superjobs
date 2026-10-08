from dataclasses import dataclass
import re

_NAME_PATTERN = re.compile(r"^[a-z0-9_-]+(?:\.[a-z0-9_-]+)*$")
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True, slots=True)
class JobIdentity:
    name: str
    version: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _NAME_PATTERN.fullmatch(self.name):
            raise ValueError(
                "Job name must contain lowercase non-empty dot-separated "
                "segments using letters, digits, '_' or '-'",
            )

        if self.version is not None:
            if not isinstance(self.version, str) or not _VERSION_PATTERN.fullmatch(self.version):
                raise ValueError(
                    "Job version must be a non-empty string without whitespace or ':'",
                )

    @property
    def canonical_name(self) -> str:
        if self.version is None:
            return self.name
        return f"{self.name}:{self.version}"

    def __str__(self) -> str:
        return self.canonical_name
