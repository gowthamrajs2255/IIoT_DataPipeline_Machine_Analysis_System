from fastapi import APIRouter, Depends

from api.db import get_connection

router = APIRouter(prefix="/machines", tags=["machines"])


@router.get("/live")
async def machine_live(conn=Depends(get_connection)):
    
    query = """
    SELECT *
    FROM machine_data
    LATEST ON timestamp PARTITION BY machine_id
    """
    rows = await conn.fetch(query)
    return [dict(r) for r in rows]


@router.get("/summary")
async def machine_summary(conn=Depends(get_connection)):
    query = """
    SELECT
        machine_id,
        avg(temperature) AS avg_temp,
        max(production_count) AS total_production,
        (sum(status) * 100.0) / count() AS utilization_pct
    FROM machine_data
    GROUP BY machine_id
    """
    rows = await conn.fetch(query)
    return [dict(r) for r in rows]


@router.get("/{machine_id}/temperature")
async def temperature_trend(machine_id: str,conn=Depends(get_connection)):
    
    query = """
    SELECT timestamp, temperature
    FROM machine_data
    WHERE machine_id = $1
    ORDER BY timestamp
    """
    rows = await conn.fetch(query,machine_id)
    
    return [dict(r) for r in rows]

@router.get("/anomalies")
async def detect_anomalies(conn=Depends(get_connection)):
    query = """
    WITH stats AS (
        SELECT 
            timestamp, 
            machine_id, 
            temperature,
            avg(temperature) OVER (PARTITION BY machine_id ORDER BY timestamp ROWS 10 PRECEDING) as moving_avg
        FROM machine_data
    )
    SELECT * FROM stats
    WHERE temperature > (moving_avg * 1.2) AND temperature > 50
    ORDER BY timestamp DESC
    LIMIT 20
    """
    rows = await conn.fetch(query)
    return [dict(r) for r in rows]