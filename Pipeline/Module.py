from enum import Enum, auto
from abc import ABCMeta, abstractmethod
from Pipeline.Exceptions import DoubleProvideException


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


class Module(metaclass=ABCMeta):
    def __init__(self):
        self.relations = {}
        self.regexpr_relations = {}
        self._skip = False  # flag to mark this module temporarily as skippable

    def create_relation(self, relation: RelationType, name, value=None):
        if name in self.relations:
            raise DoubleProvideException(f'{name} already exists in self.relations')
        self.relations[name] = {'relation': relation, 'value': ValueType(value)}

        if self.relations[name]['relation'] == RelationType.provide:
            self.relations[name]['default'] = value

        return self.relations[name]['value']

    def create_requires_by_regex(self, regex: str):
        self.regexpr_relations[regex] = {}
        return self.regexpr_relations[regex]

    def get(self, name):
        return self.relations[name]['value'].get()

    def set(self, name, value):
        self.relations[name]['value'].set(value)

    def name(self):
        return self.__class__.__name__

    @abstractmethod
    async def execute(self):
        raise NotImplementedError()

    def print_module_values(self):
        print(f'{self.name()}')
        for name, rel in self.relations.items():
            print(f'  {rel["relation"]} {name}: {rel["value"].get()}')

    def reset_provides(self):
        for rel in self.relations.values():
            if rel['relation'] == RelationType.provide:
                rel['value'].set(rel['default'])

    def skip_modules(self):
        self._skip = True

