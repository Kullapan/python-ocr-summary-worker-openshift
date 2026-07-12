"""Allow running the worker via `python -m app`."""

import asyncio

from app.worker import main

if __name__ == "__main__":
    asyncio.run(main())
