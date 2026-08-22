from dataclasses import dataclass


@dataclass(frozen=True)
class JobIdentity:
    name: str
    namespace: str | None = None
    version: str | None = None

    @staticmethod
    def default_split_namespace_name_version(
        name: str,
    ) -> tuple[str | None, str, str | None]:
        splits = name.split(".", 1)

        namespace = splits[0] if len(splits) > 1 else None
        name, version = splits[-1].split(":", -1) if ":" in splits[-1] else (splits[-1], None)

        return namespace, name, version

    @property
    def canonical_name(self) -> str:

        if self.namespace is None:
            if self.version is None:
                return self.name
            return f"{self.name}:{self.version}"
        if self.version is None:
            return f"{self.namespace}.{self.name}"
        return f"{self.namespace}.{self.name}:{self.version}"

    def __post_init__(self):

        parsed_namespace, parsed_name, parsed_version = self.default_split_namespace_name_version(self.name)

        if self.namespace is not None and parsed_namespace is not None and self.namespace != parsed_namespace:
            raise ValueError(f"Namespace mismatch: {self.namespace} != {parsed_namespace}")
        namespace = self.namespace if self.namespace is not None else parsed_namespace
        object.__setattr__(self, "namespace", namespace)

        object.__setattr__(self, "name", parsed_name)

        if self.version is not None and parsed_version is not None and self.version != parsed_version:
            raise ValueError(f"Version mismatch: {self.version} != {parsed_version}")
        version = self.version if self.version is not None else parsed_version
        object.__setattr__(self, "version", version)
