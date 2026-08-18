from dataclasses import dataclass

import pydantic


def test_type_adapter():

    @dataclass
    class X:
        a: int

    a = [1, 2, "aa"]

    d = {
        "a": 1,
        "b": [5, X(11), "7"],
        "c": "3",
    }

    dumped_a = pydantic.TypeAdapter(type(a)).dump_python(a, mode="json")
    resolved_a = pydantic.TypeAdapter(type(a)).validate_python(dumped_a)

    print(resolved_a)
    print(type(resolved_a))
    print(dumped_a)

    dumped_d = pydantic.TypeAdapter(type(d)).dump_python(d, mode="json")
    resolved_d = pydantic.TypeAdapter(type(d)).validate_python(dumped_d)

    print(resolved_d)
    print(type(resolved_d))
    print(dumped_d)

    print(pydantic.TypeAdapter(type(a)).json_schema())
    print(pydantic.TypeAdapter(type(d)).json_schema())


if __name__ == "__main__":
    test_type_adapter()
