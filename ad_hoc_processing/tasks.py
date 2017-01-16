import zmq
import logging
import base64

###########################################################################
# Task Classes for carrying out processing and delivery
###########################################################################


class Sourcing:
    """ Generates data using source function and sends to processor """
    def __init__(self, ctx, source_function, job_id, processor_address):
        ###################################################
        # Source Information and settings
        ###################################################
        self.ctx = ctx  # ZeroMQ context
        self.job_id = job_id
        self.source_function = source_function
        self.processor_address = processor_address

        # Communication
        # Set up a 0MQ socket with processor
        self.processor = self.ctx.socket(zmq.PUSH)
        self.processor.connect(self.processor_address)

        # Carry out job
        self.execute()

    def execute(self):
        # Carry out source function
        data = self.source_function(self.job_id).execute()

        # send as Base64 encoded digital inside JSON, together with job_id
        self.processor.send_json({'job_id': self.job_id,
                                  'data': base64.encodebytes(data).decode('utf-8')})
        logging.info("Source data sent to processor: %s" % self.processor_address)


class Task:
    """ Creates core (parent) object for Processing and Source threads """

    def __init__(self, ctx, parent_pipe):
        ###################################################
        # Task Information and settings
        ###################################################
        self.ctx = ctx  # ZeroMQ context
        self.parent_pipe = parent_pipe  # pipe to parent thread

        # Communication
        # Set up a 0MQ poller which can monitor multiple sockets for activity
        self.poller = zmq.Poller()
        self.poller.register(self.parent_pipe, zmq.POLLIN)

        self.task_sink = self.ctx.socket(zmq.PULL)
        self.task_port = self.task_sink.bind_to_random_port('tcp://*')  # ZMQ should choose a free port
        self.poller.register(self.task_sink, zmq.POLLIN)

        # Pass port info to parent thread to provide to Coordinator when job offered
        self.parent_pipe.send_json({'task_port': self.task_port})

    def handle_parent_message(self):
        # Gather message from pipe socket
        message_dict = self.parent_pipe.recv_json()
        # checks if sentinel message to quit
        if message_dict['message'] == "STOP":
            logging.info("Task Thread: Pipe sentinel STOP message received from parent thread")
            try:
                self.poller.unregister(self.parent_pipe)
            except:
                pass
            return True
        elif message_dict['message'] == 'job':
            self.handle_job_info(message_dict)

    def handle_job_info(self, message_dict):
        raise NotImplementedError

    def handle_task_message(self):
        raise NotImplementedError

    def execute(self):
        raise NotImplementedError

    def start(self):
        while True:

            # Collect messages from poller if available
            items = dict(self.poller.poll())  # returns dictionary

            ###########################################################################
            # Handle pipe messages sent from parent thread
            ###########################################################################
            if self.parent_pipe in items and items[self.parent_pipe] == zmq.POLLIN:
                interrupt = self.handle_parent_message()
                if interrupt:
                    break
            ###########################################################################
            # Handle task messages sent from TCP ZMQ nodes
            ###########################################################################
            if self.task_sink in items and items[self.task_sink] == zmq.POLLIN:
                self.handle_task_message()


class Processing(Task):
    """ extends core Task to allow for processing multiple binary data sources and sending to processor """
    def __init__(self, ctx, parent_pipe, process_function, is_sink):
        ###################################################
        # Task Information and settings
        Task.__init__(self, ctx, parent_pipe)
        self.processing_function = process_function
        self.is_sink = is_sink
        self.sources_expected = None
        self.sink_address = None
        self.job_id = None
        self.data = []
        logging.info("New Processor spawned")
        self.start()

    def handle_job_info(self, message_dict):
        if message_dict['message'] == 'job':
            self.job_id = message_dict['job_id']
            self.sources_expected = message_dict['sources_expected']
            self.sink_address = message_dict['sink_address']
            self.execute()

    def handle_task_message(self):
        # Extract data from JSON
        json_rcv = self.task_sink.recv_json()
        data_rcv = base64.decodebytes(json_rcv['data'].encode('utf-8'))
        self.data.append(data_rcv)
        self.execute()

    def execute(self):
        # Attempts to execute processing if conditions are met
        if len(self.data) == self.sources_expected and self.sink_address is not None:
            # Process messages
            data = self.processing_function(self.data, self.job_id).execute()

            # Connect to Sink
            sink = self.ctx.socket(zmq.PUSH)
            sink.connect(self.sink_address)
            # Send data
            sink.send_json({'job_id': self.job_id,
                            'data': base64.encodebytes(data).decode('utf-8')})
            logging.info("Processed data sent to sink: %s" % self.sink_address)

            # Inform worker to start new thread
            self.parent_pipe.send_json({'job_id': self.job_id,
                                        'status': 'sent_to_sink'})
        else:
            logging.info("%s Not enough data received (%s/%s)" % (self.job_id, len(self.data), self.sources_expected))


class Sinking(Task):
    """ extends core Task to allow for recieving processed data and saving/publishing etc """
    def __init__(self, ctx, parent_pipe, sink_function, sink_extra_params):
        ###################################################
        # Task Information and settings
        Task.__init__(self, ctx, parent_pipe)
        self.sink_function = sink_function
        self.sink_extra_params = sink_extra_params
        if self.sink_extra_params is None:
            self.sink_extra_params = {}
        self.data = None
        self.job_id = None
        logging.info("New Sink spawned")
        self.start()

    def handle_task_message(self):
        # Inform worker that is busy
        self.parent_pipe.send_json({'status': 'sending', 'job_id': self.job_id})
        # Carry out sink function on data
        # Extract data from JSON
        json_rcv = self.task_sink.recv_json()
        self.data = base64.decodebytes(json_rcv['data'].encode('utf-8'))
        self.job_id = json_rcv['job_id']
        self.execute()

    def execute(self):
        # Carry out function
        self.sink_function(self.data, self.job_id, **self.sink_extra_params).execute()
        # Inform worker that is available again
        self.parent_pipe.send_json({'status': 'sent', 'job_id': self.job_id})

    def handle_job_info(self, message_dict):
        raise NotImplementedError


