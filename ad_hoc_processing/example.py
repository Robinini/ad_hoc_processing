# Import ad_hoc_network Module
import ad_hoc_network
import plugins

# Import Modules specific to this example implementation
import random

########################################################################################
# Config
########################################################################################
is_sink = True


########################################################################################
# Source, Process and Sink Function - user extensions
########################################################################################
########################################################################################
# Temperatures - simulates thermometer
# Example of user generated Source and Process functions
# Delivery will be via standard Sink Function

class FakeTemperature(plugins.Source):
    # Completely fakes a thermometer reading and returns value
    def __init__(self, job_id):
        plugins.Source.__init__(self, job_id)

    def execute(self):
        base_temp = 19
        deviation = random.randint(-100, 100) / 100
        print("Source temperature: %s" % str(base_temp + deviation).encode("utf-8"))
        return str(base_temp + deviation).encode("utf-8")


class AverageTemperature(plugins.Process):
    # accepts list of temperatures and returns average
    def __init__(self, data, job_id):
        plugins.Process.__init__(self, data, job_id)

    def execute(self):
        if len(self.data) == 0:
            return False

        temperatures = []
        for encoded_temp in self.data:
            print("Received temperatures: %s" % encoded_temp)
            temperatures.append(float(encoded_temp.decode("utf-8")))

        average = sum(temperatures) / float(len(temperatures))
        print("Sending Average %s" % str(average).encode("utf-8"))
        return str(average).encode("utf-8")


########################################################################################
# Main Programme
########################################################################################

if __name__ == '__main__':
    # Create Node for this device
    node = ad_hoc_network.Node()

    # Create new group called "Thermometers" for this device which pretends to monitor room temperature
    # and prints average to sink node terminal
    node.new_group("Thermometers", FakeTemperature, AverageTemperature, plugins.PrintAsText, None, 3, is_sink)
