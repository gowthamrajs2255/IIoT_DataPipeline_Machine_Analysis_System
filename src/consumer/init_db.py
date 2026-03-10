import asyncio
import os
import asyncpg

async def main():
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 8812)),
        database=os.getenv("DB_NAME", "qdb"),
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS machine_data (
            machine_id SYMBOL,
            timestamp TIMESTAMP,
            power DOUBLE,
            current DOUBLE,
            temperature DOUBLE,
            production_count INT,
            status INT
        ) timestamp(timestamp) PARTITION BY DAY;
        """
    )

    print("✅ machine_data table is ready.")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
