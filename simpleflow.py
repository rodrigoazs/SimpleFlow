import pandas as pd
import yaml

class BaseData:
    def __init__(self, name):
        self.name = name

    def load(self):
        pass

    def save(self):
        pass

class CsvData(BaseData):
    def __init__(self, name, filepath, *args, **kwargs):
        super().__init__(name)
        self.filepath = filepath
        self.args = args
        self.kwargs = kwargs

    def load(self):
        return pd.read_csv(self.filepath, *self.args, **self.kwargs)

    def save(self, data):
        data.to_csv(self.filepath, index=False)

class ExcelData(BaseData):
    def __init__(self, name, filepath, *args, **kwargs):
        super().__init__(name)
        self.filepath = filepath
        self.args = args
        self.kwargs = kwargs

    def load(self):
        return pd.read_excel(self.filepath, *self.args, **self.kwargs)

    def save(self, data):
        data.to_csv(self.filepath)

class MemoryData(BaseData):
    def load(self):
        return self.data

    def save(self, data):
        self.data = data

class Node:
    def __init__(self, task, name, inputs=[], outputs=[]):
        self.task = task
        self.name = name
        self.inputs = inputs
        self.outputs = outputs

    def run(self, args):
        if len(args) == 0:
            return self.task()
        else:
            return self.task(*args)

class Pipeline:
    def __init__(self, name, nodes, datasets_config):
        self.name = name
        self.datasets = {}
        self.runned_nodes = []
        self.nodes_to_run = nodes
        self.dependencies = {}

        for node in nodes:
            dp = set()
            for ipt in node.inputs:
                for node_ in nodes:
                    if ipt in node_.outputs:
                        dp.add(node_)
            self.dependencies[node] = list(dp)

        with open(datasets_config) as file:
            result = yaml.load(file)
            for key, value in result.items():
                if value['format'] == 'MemoryData':
                    dataset = globals()[value['format']](key)
                else:
                    args = {} if 'args' not in value else value['args']
                    dataset = globals()[value['format']](key, value['path'], **args)
                self.datasets[key] = dataset

    def data(self, name):
        return self.datasets[name].load()

    def run(self):
        while len(self.nodes_to_run):
            node = self.nodes_to_run[0]
            dependencies = self.dependencies[node]
            dependencies_set = set(dependencies)
            nodes_to_run_set = set(self.nodes_to_run)
            intersct = nodes_to_run_set.intersection(dependencies_set)
            if not len(intersct):
                inputs_data = []
                for ipt in node.inputs:
                    inputs_data.append(self.datasets[ipt].load())
                outputs_names = node.outputs
                print('Executando ' + node.name)
                outputs_data = node.run(inputs_data)
                self.nodes_to_run.remove(node)
                outputs = zip(outputs_names, outputs_data)
                for output in outputs:
                    self.datasets[output[0]].save(output[1])
