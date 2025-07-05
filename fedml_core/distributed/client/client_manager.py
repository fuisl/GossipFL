import logging
from abc import abstractmethod

from mpi4py import MPI

from ..communication.message import Message
from ..communication.mpi.com_manager import MpiCommunicationManager
from ..communication.mqtt.mqtt_comm_manager import MqttCommManager
from ..communication.observer import Observer
from ..communication.grpc.grpc_comm_manager import DynamicGRPCCommManager


class ClientManager(Observer):

    def __init__(self, args, comm=None, rank=0, size=0, backend="gRPC"):
        self.args = args
        self.size = size
        self.rank = rank
        self.backend = backend
        if backend == "MPI":
            self.com_manager = MpiCommunicationManager(comm, rank, size, node_type="client")
        elif backend == "MQTT":
            HOST = "81.71.1.31"
            # HOST = "broker.emqx.io"
            PORT = 1883
            self.com_manager = MqttCommManager(HOST, PORT, client_id=rank, client_num=size - 1)
        elif backend == "gRPC":
            # Use dynamic gRPC communication manager
            if comm is None:
                raise ValueError("comm must be provided for gRPC backend")
            
            self.com_manager = comm

        else:
            self.com_manager = MpiCommunicationManager(comm, rank, size, node_type="client")
        self.com_manager.add_observer(self)
        self.message_handler_dict = dict()

    def run(self):
        self.register_message_receive_handlers()
        self.com_manager.handle_receive_message()

    def get_sender_id(self):
        return self.rank

    def receive_message(self, msg_type, msg_params) -> None:
        # logging.info("receive_message. rank_id = %d, msg_type = %s. msg_params = %s" % (
        #     self.rank, str(msg_type), str(msg_params.get_content())))
        if msg_type in self.message_handler_dict:
            handler_callback_func = self.message_handler_dict[msg_type]
            handler_callback_func(msg_params)
        else:
            logging.debug(f"No handler registered for message type: {msg_type}")
            # This is normal for RAFT/gRPC communication where some message types
            # may not have handlers registered at the ClientManager level

    def send_message(self, message):
        msg = Message()
        msg.add(Message.MSG_ARG_KEY_TYPE, message.get_type())
        msg.add(Message.MSG_ARG_KEY_SENDER, message.get_sender_id())
        msg.add(Message.MSG_ARG_KEY_RECEIVER, message.get_receiver_id())
        for key, value in message.get_params().items():
            # logging.info("%s == %s" % (key, value))
            msg.add(key, value)
        self.com_manager.send_message(msg)

    @abstractmethod
    def register_message_receive_handlers(self) -> None:
        pass

    def register_message_receive_handler(self, msg_type, handler_callback_func):
        self.message_handler_dict[msg_type] = handler_callback_func

    def finish(self):
        logging.info("__finish server")
        if self.backend == "MPI":
            MPI.COMM_WORLD.Abort()
