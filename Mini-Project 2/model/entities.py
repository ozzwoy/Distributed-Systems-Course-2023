class Book:
    def __init__(self, name, price):
        self.name = name
        self.price = price

    def __eq__(self, other):
        return (self.name, self.price) == (other.name, other.price)


class Store:
    class Entry:
        def __init__(self, book, clean):
            self.book = book
            self.clean = clean

    def __init__(self):
        self.data = []

    def add(self, book):
        for entry in self.data:
            if entry.book.name == book.name:
                entry.book.price = book.price
                entry.clean = False
                return

        self.data.append(self.Entry(book, False))

    def make_clean(self, name):
        for entry in self.data:
            if entry.book.name == name:
                entry.clean = True
                break


class Process:
    def __init__(self, node, number):
        self.id = 'Node' + str(node) + '-ps' + str(number)
        self.store = Store()
        self.successor = None
        self.predecessor = None

    def clear_store(self):
        self.store = Store()
