from Pipeline.Module import Module, RelationType
from Pipeline.Pipeline import Pipeline
from Pipeline.Exceptions import PipelineException

import asyncio


def heavy_task(val):
    j = 0
    for i in range(10000000):
        j += 1
    return val + 1


class A(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.provide, "a", 0)

    async def execute(self):
        self.a.set(1)
        self.print_module_values()
        await self.cb(heavy_task, self.a.get())


class B(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.b = self.create_relation(RelationType.provide, "b")

    async def execute(self):
        self.b.set(self.a.get()+1)
        self.print_module_values()
        await self.cb(heavy_task, self.b.get())


class C(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.c = self.create_relation(RelationType.provide, "c")

    async def execute(self):
        self.c.set(self.a.get()+2)
        self.print_module_values()
        await self.cb(heavy_task, self.c.get())


class D(Module):
    def __init__(self):
        Module.__init__(self)
        self.b = self.create_relation(RelationType.require, "b")
        self.c = self.create_relation(RelationType.require, "c")

    async def execute(self):
        self.print_module_values()
        result = await self.cb(heavy_task, self.b.get()+self.c.get())
        print(f'~D result -> {result}')


if __name__ == "__main__":
    try:
        modules = [A(), B(), C(), D()]
        p = Pipeline(modules)
        asyncio.run(p.work())
    except KeyboardInterrupt:
        print(f'User Terminate')
    except PipelineException as e:
        print(f'{e.__class__.__name__}: {e}')
    except Exception as e:
        print(e)

