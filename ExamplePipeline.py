from Pipeline.Module import Module, RelationType
from Pipeline.Pipeline import Pipeline
import asyncio


class A(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.provide, "a", 0)

    async def execute(self):
        await asyncio.sleep(0.5)
        self.print_module_values()


class B(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.b = self.create_relation(RelationType.provide, "b")

    async def execute(self):
        await asyncio.sleep(0.5)
        self.b.set(self.a.get() + 1)
        self.print_module_values()


class C(Module):
    def __init__(self):
        Module.__init__(self)
        self.a = self.create_relation(RelationType.require, "a")
        self.c = self.create_relation(RelationType.provide, "c")

    async def execute(self):
        await asyncio.sleep(0.5)
        self.c.set(self.a.get() + 2)
        self.print_module_values()


class D(Module):
    def __init__(self):
        Module.__init__(self)
        self.b = self.create_relation(RelationType.require, "b")
        self.c = self.create_relation(RelationType.require, "c")

    async def execute(self):
        self.print_module_values()


async def main():
    modules = [A(), B(), C(), D()]
    p = Pipeline(modules)
    while 1:
        e = await p.work()
        print(e)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f'User Terminate')
    except Exception as e:
        print(f'{e.__class__.__name__}: {e}')
