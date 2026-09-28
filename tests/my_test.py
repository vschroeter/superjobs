class A:
    def __init__(self):
        self.a = 1


class B:
    def __init__(self):
        self.b = 2


def main():

    a = A()
    a.a = 5

    match a:
        case A(a=1):
            print("A with a=1")
        case A():
            print("A")

        case B():
            print("B")


if __name__ == "__main__":
    main()
