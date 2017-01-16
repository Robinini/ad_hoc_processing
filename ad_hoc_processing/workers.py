import zmq
from pyre import zhelper
import uuid
import time
import urllib.parse
import logging

import tasks

###########################################################################
# Classes for managing Distributed Processing
###########################################################################


class Worker:
    """ Creates core object for Coordinator and Worker processes """
    def __init__(self, ctx, parent_pipe, uuid_int, grp_name, source_function,
                 process_function, sink_function, sink_extra_params, interval, is_sink):

        ###################################################
        # Node Information and settings
        self.ctx = ctx  # ZeroMQ context
        self.parent_pipe = parent_pipe  # pipe to parent thread
        self.uuid_int = uuid_int
        self.grp_name = grp_name
        self.source_function = source_function
        self.process_function = process_function
        self.sink_function = sink_function
        self.sink_extra_params = sink_extra_params
        self.interval = interval
        self.is_sink = is_sink

        ###################################################
        # Communication
        # Set up a 0MQ poller which can monitor multiple sockets for activity
        self.poller = zmq.Poller()
        # monitor parent socket for activity
        self.poller.register(self.parent_pipe, zmq.POLLIN)

        # Coordinator will be set up later - once known
        self.coordinator_address = None
        self.coordinator_subscription = None
        self.coordinator_pub_port = None
        self.coordinator_rep_port = None
        self.coordinator_req_pipe = None

        ###################################################
        # Initialise Task threads
        self.processor_pipe = None
        self.processor_port = None
        self.processing_busy = True
        self.create_processor()

        self.sink_pipe = None
        self.sink_port = None
        self.sink_busy = True
        if self.is_sink:
            self.create_sink()

        logging.info("%s %s object created" % (self.grp_name, self.__class__.__name__))
        self.start()

    ##################################################################################
    # Managing Group Communication & Coordinator Info
    ##################################################################################

    def merge_address(self, address, port):
        a = urllib.parse.urlparse(address)
        # Keep the same host, but change port
        return a._replace(netloc=a.hostname + ':' + str(port)).geturl()

    def connect_to_coordinator(self, message_dict):
        logging.info(self.grp_name + " Worker object connecting to Coordinator %s, PUB %s, and REP %s ports"
                     % (message_dict['coordinator_address'],
                        message_dict['coordinator_pub_port'],
                        message_dict['coordinator_rep_port']))
        # First, subscribe to this group's Coordinator publisher (can be same Node)
        self.coordinator_subscription = self.ctx.socket(zmq.SUB)
        self.coordinator_subscription.connect(self.merge_address(message_dict['coordinator_address'],
                                              message_dict['coordinator_pub_port']))
        self.coordinator_subscription.setsockopt(zmq.SUBSCRIBE, b'')  # empty string will subscribe to all messages
        self.poller.register(self.coordinator_subscription, zmq.POLLIN)

        # Second, create connect to REQ-RES Coordinator connection
        self.coordinator_req_pipe = self.ctx.socket(zmq.REP)
        self.coordinator_req_pipe.connect(self.merge_address(message_dict['coordinator_address'],
                                          message_dict['coordinator_rep_port']))
        self.poller.register(self.coordinator_req_pipe, zmq.POLLIN)

    def get_node_uuid_address(self, node_uuid_int):
        self.parent_pipe.send_json({'message': 'query_node_address', 'node_uuid_int': node_uuid_int})
        answer_dict = self.parent_pipe.recv_json()
        if answer_dict['message'] == 'query_node_address' and answer_dict['status'] == 'ok':
            return answer_dict['value']
        else:
            return 'tcp://localhost'

    def handle_parent_message(self):
        # Gather message from pipe socket
        message_dict = self.parent_pipe.recv_json()
        # checks if sentinel message to quit
        if message_dict['message'] == "STOP":
            logging.info(self.grp_name + " Pipe sentinel STOP message received from parent thread")
            self.stop()
            return True  # interrupt while loop
        else:
            if message_dict['message'] == 'coordinator_info':
                # Initiate or change connection to Coordinator Pub/Rep services
                self.connect_to_coordinator(message_dict)

    def handle_coordinator_publication(self):
        # Gather message from socket
        message_dict = self.coordinator_subscription.recv_json()
        if message_dict['message'] == 'source_trigger':
            # Generate data and send to Processor
            processor_uuid_int = message_dict['processor_uuid_int']
            processor_port = message_dict['processor_port']
            processor_address = self.get_node_uuid_address(processor_uuid_int)
            processor_address = self.merge_address(processor_address, processor_port)

            # Generate data from source
            source = tasks.Sourcing(self.ctx,
                                    self.source_function,
                                    message_dict['job_id'],
                                    processor_address
                                    )

    def handle_coordinator_reply(self):
        # Gather message from socket
        message_dict = self.coordinator_req_pipe.recv_json()
        if message_dict['message'] == 'job_offer_sink':
            # Job Offer
            job_id = message_dict['job_id']
            if self.is_sink:
                # apply for job
                self.coordinator_req_pipe.send_json({'job_id': job_id,
                                                     'uuid': self.uuid_int,
                                                     'sink_port': self.sink_port,
                                                     'message': 'accept'})
            else:
                self.coordinator_req_pipe.send_json({'job_id': job_id,
                                                     'message': 'decline'})
                logging.info("Too busy to accept sink of Job:%s" % job_id)

        if message_dict['message'] == 'job_offer_processor':
            # Job Offer
            job_id = message_dict['job_id']
            if self.processing_busy is False and self.processor_port is not None:
                # apply for job
                self.coordinator_req_pipe.send_json({'job_id': job_id,
                                                     'uuid': self.uuid_int,
                                                     'processor_port': self.processor_port,
                                                     'message': 'accept'})
                sink_uuid_int = message_dict['sink_uuid_int']
                sink_address = self.get_node_uuid_address(sink_uuid_int)
                sink_address = self.merge_address(sink_address, message_dict['sink_port'])

                self.give_job_to_processor(job_id, message_dict['sources_expected'], sink_address)

            else:
                self.coordinator_req_pipe.send_json({'job_id': job_id,
                                                     'message': 'decline'})
                logging.info("Too busy (%s, %s) to accept processing of Job:%s" %
                             (self.processing_busy, self.processor_port,job_id))

    ##################################################################################
    # Managing Tasks
    ##################################################################################

    def create_processor(self):
        self.processor_pipe = zhelper.zthread_fork(self.ctx, tasks.Processing, self.process_function, self.is_sink)
        self.poller.register(self.processor_pipe, zmq.POLLIN)

    def give_job_to_processor(self, job_id, sources_expected, sink_address):
        self.processing_busy = True
        self.processor_pipe.send_json({'message': 'job', 'job_id': job_id,
                                       'sources_expected': sources_expected,
                                       'sink_address': sink_address})

    def kill_processor(self):
        if self.processor_pipe is not None:
            try:
                self.poller.unregister(self.processor_pipe)
            except:
                pass
            self.processor_pipe.send_json({'message': 'STOP'})
            self.processor_pipe = None
            self.processing_busy = True

    def create_sink(self):
        self.sink_pipe = zhelper.zthread_fork(self.ctx, tasks.Sinking, self.sink_function, self.sink_extra_params)
        self.poller.register(self.sink_pipe, zmq.POLLIN)

    def kill_sink(self):
        if self.sink_pipe is not None:
            try:
                self.poller.unregister(self.sink_pipe)
            except:
                pass
            self.sink_pipe.send_json({'message': 'STOP'})
            self.sink_pipe = None
            self.sink_busy = True

    def handle_processor_message(self):
        # Gather message from pipe socket
        message_dict = self.processor_pipe.recv_json()
        if 'task_port' in message_dict:
            # Processor thread is advising on what port it is listening on
            self.processor_port = message_dict['task_port']
            self.processing_busy = False

        if 'job_id' in message_dict and 'status' in message_dict:
            if message_dict['status'] == 'sent_to_sink':
                # Spawn new processor
                self.kill_processor()
                self.create_processor()

    def handle_sink_message(self):
        # Gather message from pipe socket
        message_dict = self.sink_pipe.recv_json()
        if 'task_port' in message_dict:
            self.sink_port = message_dict['task_port']
            self.sink_busy = False

        if 'status' in message_dict:
            if message_dict['status'] == 'sending':
                self.sink_busy = True
            if message_dict['status'] == 'sent':
                self.sink_busy = False

    def handle_trigger(self):
        raise NotImplementedError

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
                        self.stop()
                        break

                ###########################################################################
                # Handle messages sent from Coordinator
                ###########################################################################
                # Published information
                if self.coordinator_subscription in items and items[self.coordinator_subscription] == zmq.POLLIN:
                    self.handle_coordinator_publication()

                # Responses to worker request
                if self.coordinator_req_pipe in items and items[self.coordinator_req_pipe] == zmq.POLLIN:
                    self.handle_coordinator_reply()

                ###########################################################################
                # Handle Task Messages
                ###########################################################################
                if self.processor_pipe in items and items[self.processor_pipe] == zmq.POLLIN:
                    self.handle_processor_message()
                if self.sink_pipe in items and items[self.sink_pipe] == zmq.POLLIN:
                    self.handle_sink_message()

    def stop(self):  # While loop has been exited due to STOP message
        try:
            self.poller.unregister(self.parent_pipe)
        except:
            pass
        # pass sentinel signal to all child processes
        self.kill_processor()
        self.kill_sink()
        logging.info("%s Worker object shut down" % self.grp_name)


###########################################################################
# Coordinator
###########################################################################


class Coordinator:
    """ Runs parallel to Worker - creates ZMQ PUB service for all Workers (including self) to
        publish source commands and negotiate processors and sinks with workers """

    def __init__(self, ctx, parent_pipe, uuid_int, grp_name, source_function,
                 process_function, sink_function, sink_extra_params, interval, is_sink):

        ###################################################
        # Node Information and settings
        self.ctx = ctx  # ZeroMQ context
        self.parent_pipe = parent_pipe  # pipe to parent thread
        self.uuid_int = uuid_int
        self.grp_name = grp_name
        self.source_function = source_function
        self.process_function = process_function
        self.sink_function = sink_function
        self.sink_extra_params = sink_extra_params
        self.interval = interval
        self.is_sink = is_sink

        ###################################################
        # Communication
        # Set up a 0MQ poller which can monitor multiple sockets for activity
        self.poller = zmq.Poller()
        # monitor parent socket for activity
        self.poller.register(self.parent_pipe, zmq.POLLIN)

        # Coordinator will be set up later - once known
        self.coordinator_address = None
        self.coordinator_subscription = None
        self.coordinator_pub_port = None
        self.coordinator_rep_port = None
        self.coordinator_req_pipe = None

        # create empty job list
        self.jobs = []

        # create PUB-SUB Socket to coordinate workers
        self.worker_publisher = self.ctx.socket(zmq.PUB)
        pub_port = self.worker_publisher.bind_to_random_port('tcp://*')  # ZMQ should choose a free port

        # create REQ-REP Socket to receive info from workers
        self.worker_req_pipe = self.ctx.socket(zmq.REQ)
        rep_port = self.worker_req_pipe.bind_to_random_port('tcp://*')

        # send Port info to parent thread to allow other nodes to be informed via COORDINATOR Zyre shout
        # This will allow other nodes to subscribe and request from Coordinator
        self.parent_pipe.send_json({'pub_port': pub_port, 'rep_port': rep_port})

        # Start a repeating timer thread to activate job
        self.searching_for_processor = False
        self.trigger_pipe = zhelper.zthread_fork(self.ctx, self.trigger_thread)
        self.poller.register(self.trigger_pipe, zmq.POLLIN)

        logging.info("%s Coordinator object created" % self.grp_name)
        self.start()

    def merge_address(self, address, port):
        a = urllib.parse.urlparse(address)
        # Keep the same host, but change port
        return a._replace(netloc=a.hostname + ':' + str(port)).geturl()

    def trigger_thread(self, ctx, pipe):
        while True:
            # Wait for duration of interval
            time.sleep(self.interval)
            # send trigger message to coordinator
            pipe.send("TRIGGER".encode('utf_8'))

    def handle_trigger(self):
        message = self.trigger_pipe.recv()
        if message.decode('utf-8') == 'TRIGGER':
            self.create_job()

    def create_job(self):
        if self.searching_for_processor:
            logging.warning("%s still cannot find Processor/Sink for job. Ignoring job trigger." % self.grp_name)
            return
        else:
            self.searching_for_processor = True
            job = Job(self.ctx)
            logging.info("Job created")

            # Establish how many datasets to expect
            job.sources_expected = self.get_number_nodes_in_group()

            # Establish where the Process Sink will be
            while job.sink_address is None:
                # Cycle through connected peers to see who has sink available
                self.worker_req_pipe.send_json({'message': 'job_offer_sink',
                                                'job_id': job.job_id})
                response_dict = self.worker_req_pipe.recv_json()
                if response_dict['message'] == 'accept':
                    job.sink_uuid_int = response_dict['uuid']
                    job.sink_port = response_dict['sink_port']
                    job.sink_address = self.get_node_uuid_address(job.sink_uuid_int)
                    job.sink_address = self.merge_address(job.sink_address, job.sink_port)
                    logging.info("Job accepted by Sink")
                elif response_dict['message'] == 'decline':
                    logging.info("Job declined by Sink")

            # Establish where to send data for processing
            while job.processor_address is None:
                # Cycle through connected peers to see who has processor available
                self.worker_req_pipe.send_json({'message': 'job_offer_processor',
                                                'job_id': job.job_id,
                                                'sources_expected': job.sources_expected,
                                                'sink_uuid_int': job.sink_uuid_int,
                                                'sink_port': job.sink_port
                                                })
                response_dict = self.worker_req_pipe.recv_json()
                if response_dict['message'] == 'accept':
                    job.processor_uuid_int = response_dict['uuid']
                    job.processor_port = response_dict['processor_port']
                    job.processor_address = self.get_node_uuid_address(job.processor_uuid_int)
                    job.processor_address = self.merge_address(job.processor_address, job.processor_port)

                    # Instruct all members to source data and send to processor
                    self.worker_publisher.send_json({'message': 'source_trigger',
                                                     'job_id': job.job_id,
                                                     'processor_uuid_int': job.processor_uuid_int,
                                                     'processor_port': job.processor_port
                                                     })
                    logging.info("Job accepted by Processor")
                elif response_dict['message'] == 'decline':
                    logging.info("Job declined by Processor")

            self.searching_for_processor = False
            del job  # self.jobs.append(job) # - nothing done with this - maybe will use for monitoring/testing

    def get_number_nodes_in_group(self):
        self.parent_pipe.send_json({'message': 'query_number_nodes'})
        answer_dict = self.parent_pipe.recv_json()
        if answer_dict['message'] == 'query_number_nodes' and answer_dict['status'] == 'ok':
            return int(answer_dict['value'])
        else:
            return 0

    def get_node_uuid_address(self, node_uuid_int):
        self.parent_pipe.send_json({'message': 'query_node_address', 'node_uuid_int': node_uuid_int})
        answer_dict = self.parent_pipe.recv_json()
        if answer_dict['message'] == 'query_node_address' and answer_dict['status'] == 'ok':
            return answer_dict['value']
        else:
            return 'tcp://localhost'

    def handle_parent_message(self):
        # Gather message from pipe socket
        message_dict = self.parent_pipe.recv_json()
        # checks if sentinel message to quit
        if message_dict['message'] == "STOP":
            logging.info(self.grp_name + " Pipe sentinel STOP message received from parent thread")
            self.stop()
            return True  # interrupt while loop

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
                    self.stop()
                    break

            ###########################################################################
            # Handle Trigger Messages - only Coordinator
            ###########################################################################
            if self.trigger_pipe in items and items[self.trigger_pipe] == zmq.POLLIN:
                self.handle_trigger()

    def stop(self):  # While loop has been exited due to STOP message
        try:
            self.poller.unregister(self.parent_pipe)
        except:
            pass
        # pass sentinel signal to all child processes
        logging.info("%s Coordinator object shut down" % self.grp_name)


class Job:
    """ Object to describe and help manage a Job """
    def __init__(self, ctx):
        self.ctx = ctx  # ZeroMQ context
        # create an job UUID - random UUID
        self.job_id = uuid.uuid4().int
        # Note the start time for performance analysis
        self.start_time = time.time()

        self.processor_uuid_int = None
        self.processor_port = None
        self.processor_address = None
        self.sink_uuid_int = None
        self.sink_port = None
        self.sink_address = None
        self.sources_expected = None


