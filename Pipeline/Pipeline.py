import asyncio
import gc as _gc
from Pipeline.Module import RelationType
from Pipeline.Exceptions import DoubleProvideException, CycleException


class Dependency:
    terminate = False  # flag to signal cancellation on all Modules

    def __init__(self, module, name):
        self.module = module
        self.name = name
        self.modulesAfter = []
        self.modulesBefore = []
        self.beforeEdges = 0
        self.afterEdges = 0
        self.edgesToGo = 0
        self.event = None
        self.deactivate = False  # flag to deactivate this module
        Dependency.terminate = False

    def set_event_loop(self, loop):
        asyncio.set_event_loop(loop)
        self.event = asyncio.Event()

    def add_dependency_after(self, dep):
        if not dep in self.modulesAfter:
            self.modulesAfter.append(dep)
            self.afterEdges += 1

    def add_dependency_before(self, dep):
        if not dep in self.modulesBefore:
            self.modulesBefore.append(dep)
            self.beforeEdges += 1

    @staticmethod
    def _trigger(dep):
        dep.edgesToGo -= 1
        if dep.edgesToGo == 0:
            dep.start()

    def start(self):
        self.event.set()

    def stop(self):
        self.event.clear()

    async def execute(self):
        await self.event.wait()

        if Dependency.terminate:
            return

        self.edgesToGo = self.beforeEdges + self.afterEdges

        if not self.deactivate and not self.module._skip:
            self.module.reset_provides()
            await self.module.execute()  # module.execute can set the self.module.skip flag too

        if self.deactivate or self.module._skip:
            self.skip_modules_after_this()

        self.module._skip = False

        if Dependency.terminate:
            return

        for dep in self.modulesAfter:
            self._trigger(dep)

        for dep in self.modulesBefore:
            self._trigger(dep)

        self.stop()

        if not self.modulesAfter and not self.modulesBefore:
            self.start()

    def deactivate_module(self):
        self.deactivate = True

    @classmethod
    def cancel(cls):
        Dependency.terminate = True

    def skip_modules_after_this(self):
        for d in self.modulesAfter:
            d.module._skip = True


class Pipeline:
    def __init__(self, modules, debug=False):
        self.loop = None
        self.deps = []
        self.debug = debug
        self.tasks = []
        self.run = True
        self.stop_event = None
        all_provide_rels = {}

        deps = []
        [deps.append(Dependency(m, m.name())) for m in modules]

        # hook the module dependencies together
        for d in deps:
            for name, rel in d.module.relations.items():
                for dp in deps:

                    # do not self assign dependency
                    if d == dp:
                        continue
                    for namep, relp in dp.module.relations.items():

                        # only care about modules that are related to each other
                        if name != namep:
                            continue

                        if rel['relation'] == RelationType.provide:
                            if relp['relation'] == RelationType.require:
                                d.add_dependency_after(dp)

                                # make require ValueType (and all other references) point to the provide ValueType
                                provide_val = d.module.relations[name]['value']
                                require_val = dp.module.relations[name]['value']
                                self._replace_all_refs(require_val, provide_val)
                            else:
                                raise DoubleProvideException(f'{d.name} and {dp.name} provide {name}')
                        else:
                            if relp['relation'] == RelationType.provide:
                                d.add_dependency_before(dp)

        # find cycles
        self._find_cycles(deps)

        # check if every require has a provide
        for d in deps:
            for name, rel in d.module.relations.items():
                if rel['relation'] == RelationType.require:
                    found = False
                    for dp in deps:
                        if d == dp:
                            continue
                        for namep, relp in dp.module.relations.items():
                            if relp['relation'] == RelationType.provide and name == namep:
                                found = True
                                break
                        if found:
                            break
                    if not found:
                        print(f'Nothing provides: {d.name} -> {name}')
                        d.deactivate_module()

        if self.debug:
            print_dependencies(deps)

        self.deps = deps

    @staticmethod
    def _replace_all_refs(org_obj, new_obj):
        # idea is stolen from pyjack https://github.com/cart0113/pyjack/blob/master/pyjack.py (MIT license)
        _gc.collect()
        for referrer in _gc.get_referrers(org_obj):
            for key, value in referrer.items():
                if value is org_obj:
                    value = new_obj
                    referrer[key] = value
        # finally also replace org_obj
        org_obj = new_obj

    async def _work(self, d):
        while self.run:
            await d.execute()

    async def work(self):
        print(f'Start Pipeline ...')
        self.run = True
        self.loop = asyncio.get_event_loop()
        self.tasks = [asyncio.create_task(self._work(d)) for d in self.deps]

        self.stop_event = asyncio.Event()

        # initialize dependencies
        for d in self.deps:
            d.edgesToGo = d.beforeEdges
            d.set_event_loop(self.loop)

        for d in self.deps:
            if d.edgesToGo == 0:
                d.event.set()

        done = []
        pending = []

        try:
            done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            self.run = False

            for t in self.tasks:
                if not t.cancelled():
                    t.cancel()
            self.tasks = []

            self.stop_event.set()

            for t in done:
                if t.exception():
                    return t.exception()  # return first occurred exception

    async def stop(self):
        self.run = False
        Dependency.cancel()
        for d in self.deps:
            d.start()

        self.stop_event.wait()
        print('DEBUG: Stop Pipeline!')

    def _find_cycles(self, deps):
        # based on CLRS depth-first search algorithm
        discovered = set()
        finished = set()

        for d in deps:
            if d not in discovered and d not in discovered:
                self._dfs_visit(d, d.modulesAfter, discovered, finished)

    def _dfs_visit(self, cur, deps, discovered, finished):
        discovered.add(cur)

        for d in deps:
            if d in discovered:
                raise CycleException(f"Cycle detected: {cur.name} points to {d.name}.")

            if d not in finished:
                self._dfs_visit(d, d.modulesAfter, discovered, finished)

        discovered.remove(cur)
        finished.add(cur)


def print_trigger_info(d):
    print(d.name)
    print(f'  edgesToGo: {d.edgesToGo}')
    print(f'  afteEdges: {d.afterEdges}')
    print(f'  befoEdges: {d.beforeEdges}')


def print_dependencies(deps):
    for o in deps:
        print(f'{o.name}: {id(o)}')
        print(f'  after:')
        for a in o.modulesAfter:
            print(f'    {a.name}:  {id(a)}')
        print(f'  before:')
        for b in o.modulesBefore:
            print(f'    {b.name}:  {id(b)}')

