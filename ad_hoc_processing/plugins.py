import logging
import os
import paho.mqtt.client as mqtt
import zmq
import psycopg2
import sqlite3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

################################################################
# Source, Process and Sink - core Plugins
# These provide basic methods (or just a stub) which can be
# extended by user in own applications
################################################################

################################################################
# Generate data with Source Classes


class Source:
    def __init__(self, job_id):
        self.job_id = job_id

    def execute(self):
        raise NotImplementedError
        # User plugins should return binary encoded data


class ShellSource(Source):
    def __init__(self, job_id, folder, filename, command):
        Source.__init__(self, job_id)
        self.folder = folder
        self.filename = filename
        self.command = command

    def execute(self):
        # Execute statement
        exit_code = os.system(self.command)

        path = os.path.join(self.folder, self.filename)
        # copy output file as binary data
        new_data = open(path, 'rb').read()

        # delete output file
        os.unlink(path)

        return new_data

################################################################
# Process data with Process Classes


class Process:
    def __init__(self, data, job_id):
        self.data = data
        self.job_id = job_id
        # input data should be a list containing binary data

    def execute(self):
        raise NotImplementedError
        # User plugins should return binary encoded data


class ShellProcess(Process):
    # This allows user to predefine a shell command
    # in anticipation of the command, the files for this job are saved in a folder for processing
    # as 1.dat, 2.dat... and the output is read again following shell command completion
    def __init__(self, data, job_id, input_folder, output_path, command):
        Process.__init__(self, data, job_id)
        self.input_folder = input_folder
        self.output_path = output_path
        self.command = command

    def execute(self):
        # Dump input data to folder
        num_files = len(self.data)
        files = []
        for i in range(num_files):
            path = os.path.join(self.input_folder, str(i) + ".dat")
            try:
                # Write binary data to a file
                f = open(path, 'wb')
                f.write(self.data[i])
                files.append(path)
            except:
                logging.warning(str(self.job_id) + ": Problem saving file " + path)

        # Execute statement
        exit_code = os.system(self.command)

        # Delete temporary files
        for file in files:
            os.unlink(file)

        # copy output file as binary data
        new_data = open(self.output_path, 'rb').read()

        # delete output file
        os.unlink(self.output_path)

        return new_data


################################################################
# Deliver data with Sink Classes


class Sink:
    def __init__(self, data, job_id, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.data = data
        self.job_id = job_id
        # input data should be binary data

    def execute(self):
        raise NotImplementedError


class ShellSink(Sink):
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)
        if not hasattr(self, 'command'):
            self.command = ""

    def execute(self):
        # Dump input data to folder
        path = os.path.join(self.input_folder, "sinkdata.dat")
        try:
            # Write binary data to a file
            f = open(path, 'wb')
            f.write(self.data)
        except:
            logging.warning(str(self.job_id) + ": Problem saving file " + path)
        # Execute statement
        exit_code = os.system(self.command)


class PrintAsText(Sink):
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)

    def execute(self):
        print("Job UUID: %s SinkData: %s" % (str(self.job_id), self.data))


class SaveLocal(Sink):
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)
        if not hasattr(self,'folder'):
            self.folder = "./"
        if not hasattr(self,'filename'):
            self.filename = self.job_id

    def execute(self):
        path = os.path.join(self.folder, self.filename)
        try:
            # Write binary data to a file
            f = open(path, 'wb')
            f.write(self.data)
            f.close()

        except:
            logging.warning(str(self.job_id) + ": Problem saving file " + path)


class SendMQTT(Sink):
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)
        if not hasattr(self, 'qos'):
            self.qos = 0
        self.mqttc = mqtt.Client()

    def execute(self):
        try:
            self.mqttc.connect(self.host, self.port)
            self.mqttc.publish(self.topic, self.data.decode("utf-8"), self.qos)
        except:
            logging.warning("%s: Problem publishing message with topic '%s' to MQTT server %s"
                          + (self.job_id, self.topic,self.host))


class PushZMQ(Sink):
    # Converts data to UTF-8 and pushes as string
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)

    def execute(self):
        try:
            self.push_socket.send_string(self.data.decode("utf-8"))
        except:
            logging.warning("%s: Problem pushing ZMQ message" % self.job_id)


class PubZMQ(Sink):
    # Converts data to UTF-8 and publishes as string
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)

    def execute(self):
        try:
            self.pub_socket.send_string(self.data.decode("utf-8"))
        except:
            logging.warning("%s: Problem publishing ZMQ message" % self.job_id)


class SendPostgres(Sink):
    # Converts data to UTF-8 string and submits to database
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)

    def execute(self):
        try:
            # Connect to an existing database
            conn_info = "host=" + self.host + " port=" + self.port + " dbname=" + self.database \
                      + " user=" + self.user + " password=" + self.passwd
            conn = psycopg2.connect(conn_info)

            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Create SQL command
            sql = "INSERT INTO " + self.table + " (" + self.col_jobid + "," + self.col_data + ")"
            sql += " VALUES(" + str(self.job_id) + ",'" + self.data.decode("utf-8") + "');"

            # Execute SQL command
            cur.execute(sql)

            # Commit
            conn.commit()
            # Close communication with the database
            cur.close()
            conn.close()
        except:
            logging.warning("%s: Problem updating Postgres server" % self.job_id)


class SendSQLite(Sink):
    # Converts data to UTF-8 string and submits to database
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)
        # Connect to an existing database
        self.conn = sqlite3.connect(self.database)

    def execute(self):
        try:
            # Open a cursor to perform database operations
            cur = self.conn.cursor()

            # Create SQL command
            sql = "INSERT INTO " + self.table + " (" + self.col_jobid + "," + self.col_data + ")"
            sql += " VALUES(" + str(self.job_id) + ",'" + self.data.decode("utf-8") + "');"

            # Execute SQL command
            cur.execute(sql)

            # Commit
            self.conn.commit()
            # Close communication with the database
            self.conn.close()

        except:
            logging.warning("%s: Problem updating SQlite server" % self.job_id)


class SendSMTP(Sink):
    def __init__(self, data, job_id, **kwargs):
        Sink.__init__(self, data, job_id, **kwargs)

    def execute(self):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.receiver
            msg['To'] = self.receiver
            msg['Date'] = formatdate(localtime=True)
            msg['Subject'] = "Ad-Hoc-Network Data, Job ID: " + str(self.job_id)

            msg.attach(MIMEText("See attached"))

            part = MIMEBase('application', "octet-stream")
            part.set_payload(self.data)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="Data.dat"')
            msg.attach(part)

            smtp = smtplib.SMTP(self.server, self.port)
            smtp.login(self.user, self.passwd)
            smtp.sendmail(self.receiver, self.receiver, msg.as_string())
            smtp.quit()
        except:
            logging.warning("%s: Problem sending E-Mail" % self.job_id)