from Pipeline.Module import Module, RelationType
from Pipeline.Pipeline import Pipeline
from Pipeline.Exceptions import PipelineException

import asyncio


def heavy_task(val=0):
    j = 0
    for i in range(10000000):
        j += 1

    return val + 1


class A(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.provide, "a", 0)

    async def execute(self):
        ret = await self.spawn_new_process(heavy_task, self.a.get())
        self.a.set(ret)
        self.print_module_values()


class B(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.b = self.create_relation(RelationType.provide, "b")

    async def execute(self):
        ret = await self.spawn_new_process(heavy_task, self.a.get())
        self.b.set(ret)
        self.print_module_values()


class C(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.c = self.create_relation(RelationType.provide, "c")

    async def execute(self):
        ret = await self.spawn_new_process(heavy_task, self.a.get())
        self.c.set(ret)
        self.print_module_values()


class D(Module):
    def __init__(self):
        Module.__init__(self)
        self.b = self.create_relation(RelationType.require, "b")
        self.c = self.create_relation(RelationType.require, "c")

    async def execute(self):
        await self.spawn_new_process(heavy_task)
        self.print_module_values()


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

