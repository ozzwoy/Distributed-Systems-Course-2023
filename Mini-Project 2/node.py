import config
import grpc
import shop_pb2
import shop_pb2_grpc

from concurrent import futures
from model.entities import *
from threading import Thread


class Node(shop_pb2_grpc.DistributedBookstoreServicer):
    def __init__(self, id_):
        self.id = id_
        self.processes = {}
        self.head = None
        self.head_node_id = None
        self.tail = None
        self.tail_node_id = None

    def CreateChain(self, request, context):
        return shop_pb2.CreateChainResponse(process_list=[p.id for p in self.processes])

    def Link(self, request, context):
        self.head = request.head
        self.tail = request.tail
        self.head_node_id = int(self.head[4])
        self.tail_node_id = int(self.tail[4])
        nodes = request.nodes

        for node in nodes:
            self.processes[node.process_id].successor = node.successor_id

        return shop_pb2.LinkResponse()

    def ListChain(self, request, context):
        successor = self.processes[request.process_id].successor
        successor_node = int(successor[4])

        with grpc.insecure_channel(config.IDS_TO_IPS[successor_node]) as channel:
            stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
            response = stub.ListChain(shop_pb2.ListChainRequest(process_id=successor))

        return shop_pb2.ListChainResponse(process_list=[request.process_id] + response.process_list)

    def init_processes(self, n):
        for i in range(n):
            process = Process(node=self.id, number=i + 1)
            self.processes[process.id] = process

    def create_chain(self):
        nodes_to_processes = {}

        for node in config.IDS_TO_IPS.values():
            with grpc.insecure_channel(node) as channel:
                stub = shop_pb2_grpc.DistributedBookstoreStub(channel)
                response = stub.CreateChain(shop_pb2.CreateChainRequest())


def serve():
    node_id = int(input('Enter node id: '))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = Node(node_id)
    shop_pb2_grpc.add_DistributedBookstoreServicer_to_server(node, server)
    server.add_insecure_port(config.IDS_TO_IPS[node_id])
    server.start()
    print("Server started listening\n")

    while True:
        command = input(f'Node-{node.id}> ')
        command = command.split(' ')

        if command[0] == 'Local-store-ps':
            num_processes = int(command[1])
            node.init_processes(num_processes)
        elif command[0] == 'Create-chain':
            pass
