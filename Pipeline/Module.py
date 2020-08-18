from enum import Enum, auto
from Pipeline.Exceptions import *


class RelationType(Enum):
     require = auto()
     provide = auto()


class ValueType:
    def __init__(self, _value):
        self.value = _value

    def set(self, value):
        self.value = value

    def get(self):
        return self.value


class Module:
    def __init__(self):
        self.cb = None
        self.relations = {}

    def set_process_pool_callback(self, func):
        self.cb = func

    def create_relation(self, relation: RelationType, name, value=None):
        if name in self.relations:
            raise KeyAlreadyExists(f'{name} already exists in self.relations')
        self.relations[name] = {'relation': relation, 'value': ValueType(value)}
        return self.relations[name]['value']

    def get(self, name):
        return self.relations[name]['value'].get()

    def set(self, name, value):
        self.relations[name]['value'].set(value)

    def name(self):
        return self.__class__.__name__

    async def execute(self):
        raise NotImplementedError()

    def print_module_values(self):
        print(f'{self.name()}')
        for name, rel in self.relations.items():
            print(f'  {rel["relation"]} {name}: {rel["value"].get()}')

