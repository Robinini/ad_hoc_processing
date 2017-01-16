from pyre import Pyre
from pyre import zhelper 
import zmq 
import uuid
import logging
import json
import time
import workers

###########################################################################
# Main Classes to create a cluster Node and different Groups
###########################################################################

# Note: ad_hoc_network Works in Python 3 only

###########################################################################
# Set up logging for warnings
###########################################################################
logging.basicConfig(#filename='ad_hoc.log',
                    #filemode='w',
                    level=logging.WARNING, # INFO/WARNING
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S')


###########################################################################
# Node Class to set up single zmq context for device which can create groups
###########################################################################

class Node:
    """ Main node per processor/computer. Creates zmq context and manages clusters of groups """
    def __init__(self):
        # Creates ZMQ TCP context
        self.ctx = zmq.Context()
        self.groups = []

    def new_group(self, grp_name, source_function, process_function, sink_function, sink_extra_params,
                  interval=1, is_sink=False):
        # Create new group object and adds pipe to List
        group_pipe = zhelper.zthread_fork(self.ctx, Group, grp_name, 
                                          source_function, process_function, sink_function, sink_extra_params,
                                          interval, is_sink)
        self.groups.append(group_pipe)

    def close_group(self, grp_name):
        # Forces group of given name to close
        for group in self.groups:
            if group.grp_name == grp_name:
                # Sends a sentinel signal to this group to shut it down
                group.send("STOP".encode('utf-8'))
                

###########################################################################
# Group Class for Clustering & Leader Electing
###########################################################################

class Group:
    """Creates a group cluster object to carry out specified task and upload results to server
       typically uses Zyre processes and communication as opposed to ZMQ directly """

    def __init__(self, ctx, parent_pipe, grp_name,
                 source_function, process_function, sink_function, sink_extra_params,
                 interval, is_sink=False):

        # Arguments
        self.ctx = ctx  # ZeroMQ context
        self.parent_pipe = parent_pipe  # pipe to parent thread
        self.grp_name = grp_name  # cluster name for use in Pyre Node name and Pyre Cluster Group Name
        self.source_function = source_function
        self.process_function = process_function
        self.sink_function = sink_function
        self.sink_extra_params = sink_extra_params
        self.interval = interval
        self.is_sink = is_sink

        ##############################################
        # create a new Zyre node
        self.n = Pyre(self.grp_name)  # Constructor

        self.n.join(self.grp_name) #  Join a named group to send/rc group messages, can do after start also
        self.n.start()  # begins discovery and connection

        # Set up a 0MQ poller which can monitor multiple sockets for activity
        # Repeatedly checks something till certain event/State occurrs
        self.poller = zmq.Poller() # Set up new instance

        # Register the 0MQ socket for I/O monitoring
        # POLLIN - at least one message may be RECEIVED from the socket without blocking.
        self.poller.register(self.parent_pipe, zmq.POLLIN)  # inter-Thread communication with parent thread
        self.poller.register(self.n.socket(), zmq.POLLIN)  # Group communication through Pyre Node

        # Display our node UUID integer, after successful initialization
        self.uuid_int = self.n.uuid().int
        logging.info("%s This node UUID Integer %s" % (self.grp_name, self.uuid_int))

        # This node has just joined the group - call an election
        # Wait a moment to allow peers to be recognised
        time.sleep(2)
        self.election = None
        self.coordinator_uuid_int = None  # UUID integer of current group Coordinator
        # Communication as Coordinator on which coordination info is published
        self.coordinator_pub_port = None
        self.coordinator_pipe = None
        self.coordinator_rep_port = None
        # Communication as Worker on which coordination info is published
        self.worker_pipe = None

        # Initiate election
        self.call_election()
        # Start monitoring
        self.start()

    ############################################################################
    # Leader Election functions
    ###########################################################################

    def get_num_peers(self):
        # Provides number of peers in group
        peers = self.n.peers_by_group(self.grp_name)
        return len(peers)

    def get_node_address(self, node_uuid_int):
        # peers does not contain own node, therefore first check against own id
        if node_uuid_int == self.uuid_int:
            return 'tcp://localhost'  # default value

        peers = self.n.peers_by_group(self.grp_name)
        for peer in peers:
            if peer.int == node_uuid_int:
                return self.n.peer_address(peer)
        logging.warning("Function get_node_address failed to find address")
        return 'tcp://localhost'

    def get_superior_peers(self): # Returns list of current peers with higher process IDs
        peers = self.n.peers_by_group(self.grp_name)
        superiors = []
        for peer in peers:  # UUID Objects as list
            # Assumes peers lists just peers in this group (as with Zyre)
            if peer.int > self.uuid_int:  # Higher UUID
                superiors.append(peer)
        return superiors

    def call_election(self):
        # carries out own election if not currently running
        if self.election is not None:
            return

        # updates coordinator_uuid_int ONLY if no other peers available
        # otherwise starts election process and adds new election info to parent elections list

        # Checks list of current peers
        superiors = self.get_superior_peers()
        if len(superiors) == 0:
            # This is the only Node/most superior node in the group - will elect self as leader
            group_size = self.get_num_peers() + 1
            logging.info("This is the only Node/most superior Node in the group (%s) of %s - electing self"
                         % (self.grp_name, group_size))
            self.declare_self_coordinator()
            return

        # Otherwise hold election
        logging.info("%s superior peers in %s - instigating election" % (len(superiors), self.grp_name))

        # Note: Likely to be many concurrent elections
        # therefore create an election UUID - random UUID
        election_id = uuid.uuid4().int

        message = json.dumps({'electionID': election_id, 'message': 'NOMINATE'})
        # This method uses pure Zyre
        for peer in superiors:  # UUID Objects as list
            self.n.whispers(peer, message)

        # Start a timer thread
        election_pipe = zhelper.zthread_fork(self.ctx, self.close_voting)
        # Registers PIPE to allow inter-Thread communication with parent thread
        self.poller.register(election_pipe, zmq.POLLIN)

        # Update this Node election information
        self.election = [election_id, election_pipe]

    def close_voting(self, ctx, pipe):
        delay = 10  # (s) how long do we allow Nodes to reply to NOMINATE before assuming Node is no longer in Group

        # Wait a while
        time.sleep(delay)

        # Once complete, send Pyre message back through pipe
        #  to parent loop which should close this election
        try:
            pipe.send("STOP".encode('utf_8'))
        except:
            logging.info("%s Election called off by Node before closure" % self.grp_name)

    def kill_election(self):
        if self.election is not None:
            self.poller.unregister(self.election[1])  # This is important to avoid ghost messages
            self.election = None

    def accept_nomination(self, messageJSON, peer):
        message = json.dumps({'electionID': messageJSON['electionID'], 'message': 'OK'})
        # Answer using Zyre whisper
        self.n.whispers(peer, message)
        # Call own election - function will check if one exists already
        self.call_election()

    def declare_self_coordinator(self):
        # This Node declares itself Coordinator
        self.kill_election()

        # Update variables
        if self.coordinator_uuid_int != self.uuid_int:
            logging.info("This node is the new Coordinator of %s" % self.grp_name)
            self.coordinator_uuid_int = self.uuid_int
            # Sets up Coordinator ZMQ Pub-Sub Thread and saves service port to self.coordinator_pub_port
            self.create_coordinator()
            # Creates paralel worker thread
            self.create_worker()
        else:
            logging.info("This node remains as Coordinator of %s" % self.grp_name)

        # Communicate to group using Zyre shout
        if self.get_num_peers() > 0:
            message = {'message': 'COORDINATOR',
                       'pub_port': self.coordinator_pub_port,
                       'rep_port': self.coordinator_rep_port}
            self.n.shouts(self.grp_name, json.dumps(message))

    def handle_parent_message(self):
        # Gather message from pipe socket
        message = self.parent_pipe.recv()
        # checks if sentinel message to quit
        if message.decode('utf-8') == "STOP":
            logging.info("%s Pipe sentinel STOP message received from parent thread" % self.grp_name)
            self.stop()
            return True
        else:
            logging.info("%s Message from parent thread: %s" % (self.grp_name,message.decode('utf-8')))

    def handle_election_close(self, timer_pipe):
        message = timer_pipe.recv()
        if message.decode('utf-8') == "STOP":
            # election has closed voting and still exists - declare self winner
            logging.info("%s Election ID - %s ended" % (self.grp_name, self.election[0]))
            self.declare_self_coordinator()  # Informs all other nodes in group

    def handle_topology_changes(self, msg_type, cmds):
        if msg_type.decode('utf-8') == "ENTER":
            # New peer (or self) has joined cluster, but not necessarily group
            pass  # logging.info("New peer has entered cluster network")
        if msg_type.decode('utf-8') == "EXIT" or msg_type.decode('utf-8') == "LEAVE":
            # Peer (or self) has left cluster/group or remained inactive for 30s
            # Initiate Election if Coordinator Node has left
            if self.coordinator_uuid_int == uuid.UUID(bytes=cmds[1]).int:
                # Call an election - the coordinator has gone
                self.call_election()
        if msg_type.decode('utf-8') == "JOIN":
            # Peer (or self) has joined group.
            # Call an election - this may be re-entering after a period of absence
            self.call_election()

    def handle_shout_info(self, cmds):
        # Peer (not self) has sent multicast message - likely coordinator asserting position
        # eg: [b'SHOUT', b'\xc8\xb9q...91\x1b=;', b'Cluster', b'Cluster', b'{"message": "COORDINATOR"}']
        try:
            message_dict = json.loads(cmds[4].decode('utf-8'))
            if message_dict['message'] == 'COORDINATOR':
                # A coordinator has been appointed - cancel all elections
                self.kill_election()
                # Update coordinator variable
                if self.coordinator_uuid_int != uuid.UUID(bytes=cmds[1]).int:
                    if self.coordinator_uuid_int == self.uuid_int:
                        # If this Node currently is the Coordinator - kill Coordinator Process
                        self.kill_coordinator()
                    # Coordinator has changed - reconfigure worker
                    self.coordinator_uuid_int = uuid.UUID(bytes=cmds[1]).int
                    self.coordinator_pub_port = message_dict['pub_port']
                    self.coordinator_rep_port = message_dict['rep_port']
                    logging.info("%s A new Coordinator has been appointed: %s"
                                 % (self.grp_name, self.coordinator_uuid_int))
                    # subscribe/change worker to new Coordinator
                    self.create_worker()
                else:
                    logging.info("%s Coordinator %s remains in charge" % (self.grp_name, self.coordinator_uuid_int))
        except:
            log_msg = "%s Non JSON SHOUT Message received: %s" % (self.grp_name, cmds[4].decode('utf-8'))
            logging.info(log_msg)

    def handle_whisper_info(self, cmds):
        # Peer has sent this Node a 1-1 message - likely to be election nomination or acceptance
        # eg: [b'WHISPER', ..., b'{"electionID": 238824...6925651, "message": "NOMINATE"}']
        try:
            message_dict = json.loads(cmds[3].decode('utf-8'))
            if message_dict['message'] == 'NOMINATE':  # a Node is nominating this node for election
                # send acceptance back
                self.accept_nomination(message_dict, uuid.UUID(bytes=cmds[1]))
                logging.info("%s An election nomination from an inferior peer has been accepted by this node"
                             % self.grp_name)

            if message_dict['message'] == 'OK':  # Someone has accepted our nomination
                if self.election is not None:
                    if message_dict['electionID'] == self.election[0]:
                        # election is now redundant - remove
                        self.kill_election()
                        logging.info("%s A nomination has been accepted by a superior peer for this ElectionID: %s" %
                                     (self.grp_name, message_dict['electionID']))
        except:
            log_msg = "%s Non JSON WHISPER Message received: %s" % (self.grp_name, cmds[3].decode('utf-8'))
            logging.info(log_msg)

    ############################################################################
    # Distributed processing functions
    ###########################################################################

    def create_coordinator(self):
        # This node is newly confirmed as Coordinator Coordinator - Start publication thread
        # if no thread, create one, if thread, change target and restart

        self.coordinator_pipe = zhelper.zthread_fork(self.ctx,
                                                     workers.Coordinator,
                                                     self.uuid_int,
                                                     self.grp_name,
                                                     self.source_function,
                                                     self.process_function,
                                                     self.sink_function,
                                                     self.sink_extra_params,
                                                     self.interval,
                                                     self.is_sink)

        # establish which port PUB/REP available from. This should be first message from pipe to Coordinator thread
        message_dict = self.coordinator_pipe.recv_json()
        self.coordinator_pub_port = message_dict['pub_port']
        self.coordinator_rep_port = message_dict['rep_port']
        self.poller.register(self.coordinator_pipe, zmq.POLLIN)  # For queries from Coordinator

    def handle_coordinator_message(self):
        # Gather message from pipe socket
        message_dict = self.coordinator_pipe.recv_json()
        if message_dict['message'] == 'query_number_nodes':
            number_nodes = self.get_num_peers() + 1
            self.coordinator_pipe.send_json({'message': 'query_number_nodes', 'status':'ok', 'value': number_nodes})
        if message_dict['message'] == 'query_node_address':
            node_uuid_int = message_dict['node_uuid_int']
            node_address = self.get_node_address(node_uuid_int)
            self.coordinator_pipe.send_json({'message': 'query_node_address', 'status': 'ok', 'value': node_address})

    def kill_coordinator(self):
        if self.coordinator_pipe is not None:
            try:
                self.poller.unregister(self.coordinator_pipe)
            except:
                pass
            self.coordinator_pipe.send_json({'message': 'STOP'})
            self.coordinator_pipe = None

    def create_worker(self):
        # This node has new information regarding a new Coordinator
        # if no worker thread, create one, if thread exists, get it to reconfigure coordination sockets
        if self.worker_pipe is None:
            self.worker_pipe = zhelper.zthread_fork(self.ctx,
                                                    workers.Worker,
                                                    self.uuid_int,
                                                    self.grp_name,
                                                    self.source_function,
                                                    self.process_function,
                                                    self.sink_function,
                                                    self.sink_extra_params,
                                                    self.interval,
                                                    self.is_sink)
        self.poller.register(self.worker_pipe, zmq.POLLIN)  # For queries from Worker

        # send connection info
        self.worker_pipe.send_json({'message': 'coordinator_info',
                              'coordinator_address': self.get_node_address(self.coordinator_uuid_int),
                              'coordinator_pub_port': self.coordinator_pub_port,
                              'coordinator_rep_port': self.coordinator_rep_port})


    def handle_worker_message(self):
        # Gather message from pipe socket
        message_dict = self.worker_pipe.recv_json()
        if message_dict['message'] == 'query_node_address':
            node_uuid_int = message_dict['node_uuid_int']
            node_address = self.get_node_address(node_uuid_int)
            self.worker_pipe.send_json({'message': 'query_node_address', 'status': 'ok', 'value': node_address})

    def kill_worker(self):
        if self.worker_pipe is not None:
            try:
                self.poller.unregister(self.worker_pipe)
            except:
                pass
            self.worker_pipe.send_json({'message': 'STOP'})
            self.worker_pipe = None

    ############################################################################
    # Loop - monitoring messages continually
    ###########################################################################

    def start(self):
        while True:
                # Collect messages from poller if available (will BLOCK unless message received)
                items = dict(self.poller.poll())

                ###########################################################################
                # Handle pipe messages sent from parent thread
                ###########################################################################
                if self.parent_pipe in items and items[self.parent_pipe] == zmq.POLLIN:
                    interrupt = self.handle_parent_message()
                    if interrupt:
                        break

                ###########################################################################
                # Handle pipe messages sent from Worker and Coordinator threads
                ###########################################################################
                if self.coordinator_pipe in items and items[self.coordinator_pipe] == zmq.POLLIN:
                    self.handle_coordinator_message()

                if self.worker_pipe in items and items[self.worker_pipe] == zmq.POLLIN:
                    self.handle_worker_message()

                ###########################################################################
                # Handle messages from Pyre Nodes in Group
                ###########################################################################
                if self.n.socket() in items and items[self.n.socket()] == zmq.POLLIN:  # message is from Pyre Node
                    # Handle messages received from (SHOUT, WHISPER) or Cluster Info (ENTER, EXIT, JOIN, LEAVE)
                    # Gather message from zyre node
                    cmds = self.n.recv()
                    # returns List: [message type,sender ID,Node Name,group name,(headers if ENTER), message]
                    # Establish type of message
                    msg_type = cmds[0]

                    ###########################################################################
                    # Handle Topology changes (ENTER, EXIT, JOIN, LEAVE, EVASIVE )
                    # # (messages are queued/reissued from current members upon JOIN)
                    if msg_type.decode('utf-8') in ("ENTER", "EXIT", "JOIN", "LEAVE", "EVASIVE"):
                        self.handle_topology_changes(msg_type, cmds)

                    ###########################################################################
                    # Handle SHOUT and WHISPER - election messages (these are not queued prior to JOIN by the way)
                    if msg_type.decode('utf-8') == "SHOUT":
                        self.handle_shout_info(cmds)

                    if msg_type.decode('utf-8') == "WHISPER":
                        self.handle_whisper_info(cmds)

                ###########################################################################
                # Handle pipe messages sent from own election closing vote notification
                ###########################################################################
                if self.election is not None:
                    timer_pipe = self.election[1]
                    if timer_pipe in items and items[timer_pipe] == zmq.POLLIN:  # message is from own election
                        self.handle_election_close(timer_pipe)

    def stop(self):  # While loop has been exited due to STOP sentinel
        self.n.stop()  # Exit group gracefully
        # Send stop sentinel to Worker
        if self.coordinator_pipe is not None:
            self.kill_coordinator()
        if self.worker_pipe is not None:
            self.kill_worker()
        logging.info("%s Group shut down" % self.grp_name)

