import asyncio
import concurrent.futures
from Pipeline.Module import RelationType
from Pipeline.Exceptions import *
import gc as _gc
import signal


class Dependency:
    def __init__(self, module, name):
        self.module = module
        self.name = name
        self.modulesAfter = []
        self.modulesBefore = []
        self.beforeEdges = 0
        self.afterEdges = 0
        self.edgesToGo = 0
        self.event = None
        self.deactivated = False

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

        if self.deactivated:
            return

        self.edgesToGo = self.beforeEdges + self.afterEdges

        await self.module.execute()

        for dep in self.modulesAfter:
            self._trigger(dep)

        for dep in self.modulesBefore:
            self._trigger(dep)

        self.stop()

        if not self.modulesAfter and not self.modulesBefore:
            self.start()

    def terminate(self):
        self.deactivated = True
        self.stop()


class Pipeline:
    def __init__(self, modules, max_workers=8):
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
        self.loop = None
        self.deps = []
        self.run = True

        deps = []
        [deps.append(Dependency(m, m.name())) for m in modules]

        for d in deps:
            d.module.set_process_pool_callback(self._call_in_process_pool)

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
                        raise NoProvide(f'Nothing provides: {d.name} -> {name}')

        self.deps = deps
        print_dependencies(deps)

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

    async def _call_in_process_pool(self, func, *args):
       return (await asyncio.gather(*[self.loop.run_in_executor(self.executor, func, *args)], loop=self.loop))[0]

    def shutdown(self):
        # Please note: the shutdown handler is called for every subprocess. This is not nice, but also not critical
        self.run = False
        for d in self.deps:
            d.terminate()
        self.executor.shutdown()

    async def _work(self, d):
        while self.run:
            await d.execute()

    async def work(self):
        print(f'Start Pipeline ...')
        self.loop = asyncio.get_event_loop()
        self.loop.add_signal_handler(signal.SIGINT, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)

        jobs = [self._work(d) for d in self.deps]

        # initialize dependencies
        for d in self.deps:
            d.edgesToGo = d.beforeEdges
            d.set_event_loop(self.loop)

        for d in self.deps:
            if d.edgesToGo == 0:
                d.event.set()

        await asyncio.gather(*jobs, loop=self.loop)

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



#todo:
# -make Module's getter setter easier to use
# -measure modules execution time
