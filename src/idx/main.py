from idx.download import download_selection_lists
from prefect import flow, get_run_logger, task
import asyncio

async def main():
    # _download_task = task(download_selection_lists, retries=3, retry_delay_seconds=5)  # type: ignore[call-overload]
    await download_selection_lists() # comment out later.



if __name__ == "__main__":
    asyncio.run(main())
