import config
import grpc
import random
import shop_pb2
import shop_pb2_grpc

from concurrent import futures
from model.entities import *
from threading import Thread


class Node(shop_pb2_grpc.DistributedBookstoreServicer):
    def __init__(self, id_):
        self.id = id_
        self.ids_to_processes = {}
        self.head = None
        self.head_node_id = None
        self.tail = None
        self.tail_node_id = None

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
        elif command[0] == 'Show-processes':
            for p in node.ids_to_processes.values():
                print(f'{p.id} -> {p.successor}')
        else:
            print('Wrong command! Please try again.')


if __name__ == '__main__':
    serve()
