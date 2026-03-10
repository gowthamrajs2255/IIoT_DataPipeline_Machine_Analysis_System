import asyncpg

pool: asyncpg.pool.Pool | None = None


async def create_pool():
    """Create a connection pool to QuestDB (Postgres wire protocol)."""
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            user="admin",
            password="quest",
            database="qdb",
            host="questdb",
            port=8812,
            min_size=1,
            max_size=10,
        )


async def close_pool():
    """Close the global pool, if it exists."""
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def get_connection():
    """FastAPI dependency that yields a connection from the pool."""
    if pool is None:
        raise RuntimeError("Connection pool not initialized")
    async with pool.acquire() as connection:
        yield connection
