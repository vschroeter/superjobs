from dataclasses import dataclass

@dataclass(frozen=True)
class JobIdentity:
    name: str
    namespace: str | None = None
    version: int = 0

    @staticmethod
    def default_split_namespace_and_name(name: str) -> tuple[str, str]:
        splits = name.split(".", 1)
        if len(splits) == 1:
            return None, splits[0]
        return splits[0], splits[1]

    @property
    def canonical_name(self) -> str:
        if self.namespace is None:
            return f"{self.name}:{self.version}"
        return f"{self.namespace}.{self.name}:v{self.version}"

    def __post_init__(self):
        if self.namespace is None:
            namespace, name = self.default_split_namespace_and_name(self.name)
            object.__setattr__(self, "namespace", namespace)
            object.__setattr__(self, "name", name)
        if self.name is None:
            namespace, name = self.default_split_namespace_and_name(self.name)
            object.__setattr__(self, "name", name)
            object.__setattr__(self, "namespace", namespace)
