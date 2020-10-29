import asyncio


async def wait(tasks:asyncio.Task):
    """
    Wrapper for asyncio.wait().
    - cancel all tasks on error
    - reraises first exception
    """
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for t in pending:
        try:
            t.cancel()
            await t
        except:
            # ignore all further exceptions during cleanup
            pass

    for t in done:
        if t.exception():
            raise t.exception()


