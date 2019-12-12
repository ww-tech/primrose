from primrose.base.node import AbstractNode

class TestExtNode(AbstractNode):
    
    def necessary_config(self, node_config):
        return ([])

    def run(self, data_object):
        terminate = False
        return data_object, terminate
