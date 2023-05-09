import config
import shop_pb2
import shop_pb2_grpc

import grpc
import random
import threading

from concurrent import futures
from model.entities import *


class Node(shop_pb2_grpc.DistributedBookstoreServicer):
    def __init__(self, id_):
        self.id = id_
        self.ids_to_processes = {}
        self.head = None
        self.head_node_id = None
        self.tail = None
        self.tail_node_id = None
        self.timeout = 0

    def CreateChain(self, request, context):
        return shop_pb2.CreateChainResponse(process_list=[p.id for p in self.ids_to_processes.values()])

    def Link(self, request, context):
        self.head = request.head
        self.tail = request.tail
        self.head_node_id = int(self.head[4])
        self.tail_node_id = int(self.tail[4])
        chain_nodes = request.chain_nodes

        for node in chain_nodes:
            successor = node.successor_id if len(node.successor_id) > 0 else None
            predecessor = node.predecessor_id if len(node.predecessor_id) > 0 else None
            self.ids_to_processes[node.process_id].successor = successor
            self.ids_to_processes[node.process_id].predecessor = predecessor

        return shop_pb2.LinkResponse()

    def ListChain(self, request, context):
        return shop_pb2.ListChainResponse(chain_nodes=[shop_pb2.ChainNode(process_id=p.id, successor_id=p.successor,
                                                                          predecessor_id=p.predecessor)
                                                       for p in self.ids_to_processes.values()])

    def ListBooks(self, request, context):
        return shop_pb2.ListBooksResponse(books=[shop_pb2.Book(name=entry.book.name, price=entry.book.price) for entry
                                                 in self.ids_to_processes[self.tail].store.data])

    def Read(self, request, context):
        # this request is supposed to be responded by the tail
        books = [entry.book for entry in self.ids_to_processes[self.tail].store.data]
        found = next((book for book in books if book.name == request.name), None)

        if found is not None:
            return shop_pb2.ReadResponse(book=shop_pb2.Book(name=found.name, price=found.price))
        else:
            return shop_pb2.ReadResponse(book=shop_pb2.Book(name='', price=0))

    def Write(self, request, context):
        process = self.ids_to_processes[request.process_id]
        book = Book(request.name, request.price)
        process.store.add(book)

        if process.successor is not None:
            timer = threading.Timer(self.timeout, self.write_func, args=[process.successor, book])
        else:
            process.store.make_clean(book.name)
            timer = threading.Timer(0, self.clean_func, args=[process.predecessor, book])
        timer.start()

        return shop_pb2.WriteResponse()

    def Clean(self, request, context):
        process = self.ids_to_processes[request.process_id]
        found = next((entry for entry in process.store.data if entry.book.name == request.book.name), None)

        if found is not None and found.book.price == request.book.price:
            found.clean = True

            if process.predecessor is not None:
                timer = threading.Timer(self.timeout, self.clean_func, args=[process.predecessor, found.book])
                timer.start()

        return shop_pb2.CleanResponse()

    def SetTimeout(self, request, context):
        self.timeout = request.timeout

        return shop_pb2.SetTimeoutResponse()

    def init_processes(self, n):
        for i in range(n):
            process = Process(node=self.id, number=i + 1)
            self.ids_to_processes[process.id] = process

    def create_chain(self):
        processes = []
        processes_to_nodes = {}

        # creating new chain removes all previously stored data
        self.clear_store()

        for node in config.IDS_TO_IPS.values():
            with grpc.insecure_channel(node) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                response = stub.CreateChain(shop_pb2.CreateChainRequest())
                processes += response.process_list

                for process in response.process_list:
                    processes_to_nodes[process] = node

        random.shuffle(processes)

        nodes_to_chain_nodes = {}
        for node in config.IDS_TO_IPS.values():
            nodes_to_chain_nodes[node] = []

        for i in range(len(processes)):
            if i != 0 and i != len(processes) - 1:
                chain_node = shop_pb2.ChainNode(process_id=processes[i], successor_id=processes[i + 1], predecessor_id=processes[i - 1])
            elif i == 0:
                chain_node = shop_pb2.ChainNode(process_id=processes[i], successor_id=processes[i + 1], predecessor_id='')
            else:
                chain_node = shop_pb2.ChainNode(process_id=processes[i], successor_id='', predecessor_id=processes[i - 1])
            nodes_to_chain_nodes[processes_to_nodes[processes[i]]].append(chain_node)

        for node in config.IDS_TO_IPS.values():
            with grpc.insecure_channel(node) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                stub.Link(shop_pb2.LinkRequest(head=processes[0], tail=processes[-1],
                                               chain_nodes=nodes_to_chain_nodes[node]))

    def clear_store(self):
        if len(self.ids_to_processes) != 0:
            for process in self.ids_to_processes.values():
                process.clear_store()

    def list_chain(self):
        if self.head is None:
            return []

        chain_nodes = []

        for node in config.IDS_TO_IPS.values():
            with grpc.insecure_channel(node) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                response = stub.ListChain(shop_pb2.ListChainRequest())
                chain_nodes += response.chain_nodes

        head = next((node for node in chain_nodes if node.predecessor_id == ''), None)
        current = head
        result = []

        while current is not None:
            result.append(current.process_id)
            current = next((node for node in chain_nodes if node.process_id == current.successor_id), None)

        result[0] += '(Head)'
        result[-1] += '(Tail)'

        return ' -> '.join(result)

    def list_books(self):
        # we must not return dirty data, so we ask the tail to provide the clean data
        with grpc.insecure_channel(config.IDS_TO_IPS[self.tail_node_id]) as channel:
            stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
            response = stub.ListBooks(shop_pb2.ListBooksRequest())

        return [Book(book.name, book.price) for book in response.books]

    def read(self, name):
        entries = list(self.ids_to_processes.values())[0].store.data
        found = next((entry for entry in entries if entry.book.name == name), None)

        if found is None:
            return None
        elif found.clean:
            return found.book
        else:
            # we must not return dirty data, so we ask the tail to provide the clean data
            with grpc.insecure_channel(config.IDS_TO_IPS[self.tail_node_id]) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                response = stub.Read(shop_pb2.ReadRequest(name=name))

            return Book(response.book.name, response.book.price)

    def write(self, book):
        # start the chain of replication of the head
        self.write_func(self.head, book)

    def write_func(self, process_id, book):
        node = int(process_id[4])
        with grpc.insecure_channel(config.IDS_TO_IPS[node]) as channel:
            stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
            stub.Write(shop_pb2.WriteRequest(process_id=process_id, name=book.name, price=book.price))

    def clean_func(self, process_id, book):
        node = int(process_id[4])
        with grpc.insecure_channel(config.IDS_TO_IPS[node]) as channel:
            stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
            stub.Clean(shop_pb2.CleanRequest(process_id=process_id, book=shop_pb2.Book(name=book.name, price=book.price)))

    def set_timeout(self, timeout):
        for node in config.IDS_TO_IPS.values():
            with grpc.insecure_channel(node) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                stub.SetTimeout(shop_pb2.SetTimeoutRequest(timeout=timeout))

    def data_status(self, i):
        return list(self.ids_to_processes.values())[i].store.data


def serve():
    node_id = int(input('Enter node id: '))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = Node(node_id)
    shop_pb2_grpc.add_DistributedBookstoreServicer_to_server(node, server)
    server.add_insecure_port(config.IDS_TO_IPS[node_id])
    server.start()
    print("Server started listening.\n")

    while True:
        command = input(f'Node-{node.id}> ')
        command = command.split(' ')

        if command[0] == 'Local-store-ps':
            num_processes = int(command[1])
            node.init_processes(num_processes)
        elif command[0] == 'Create-chain':
            if node.head is None:
                node.create_chain()
            else:
                command = input('A chain is already created. Creating new chain will destroy all stored data. ' 
                                'Are you sure you want to create a new chain? yes/no: ')
                if command == 'yes':
                    node.create_chain()
                elif command == 'no':
                    continue
                else:
                    print('Wrong command! Please try again.')
        elif command[0] == 'List-chain':
            print(node.list_chain())
        elif command[0] == 'List-books':
            books = node.list_books()
            for i, book in enumerate(books):
                print(f'{i + 1}) {book.name} = {round(book.price, 1)} EUR')
            if len(books) == 0:
                print('No books yet in the stock.')
        elif command[0] == 'Read-operation':
            command = ' '.join(command[1:])
            name = command[1:-1]
            book = node.read(name)
            if book is not None:
                print(f'{round(book.price, 1)} EUR')
            else:
                print('Not yet in the stock.')
        elif command[0] == 'Write-operation':
            command = ' '.join(command[1:])
            command = command.split('"')
            name = command[1]
            price = round(float(command[2][2:-1]), 1)
            node.write(Book(name, price))
        elif command[0] == 'Time-out':
            timeout = int(command[1])
            node.set_timeout(timeout)
        elif command[0] == 'Data-status':
            p = int(command[1]) - 1
            data = node.data_status(p)
            for i, entry in enumerate(data):
                print(f'{i + 1}) {entry.book.name} -- {"clean" if entry.clean else "dirty"}')
        else:
            print('Wrong command! Please try again.')


if __name__ == '__main__':
    serve()
