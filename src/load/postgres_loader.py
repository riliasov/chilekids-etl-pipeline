import json
from typing import List, Dict, Any
from ..utils.db import executemany, executemany_one_off


async def load_raw(table: str, rows: List[Dict[str, Any]], batch_size: int = 1000) -> None:
    """Загружает строки в raw.data.

    asyncpg требует, чтобы значения JSON/JSONB передавались как текст для подготовленных
    выражений, поэтому мы сериализуем payload в JSON-строку и приводим к jsonb в SQL,
    если это необходимо на стороне сервера.
    """
    if not rows:
        return
    # Explicitly cast $3 to jsonb to avoid type issues when passing a string
    sql = f"INSERT INTO raw.data (id, source, payload) VALUES ($1, $2, $3::jsonb) ON CONFLICT (id) DO NOTHING"
    def args_iter():
        for r in rows:
            payload = r.get('payload')
            if not isinstance(payload, str):
                payload = json.dumps(payload, ensure_ascii=False)
            yield (r.get('id'), table, payload)

    batch = []
    for a in args_iter():
        batch.append(a)
        if len(batch) >= batch_size:
            # use the connection pool instead of one-off connections
            await executemany(sql, batch)
            batch = []
    if batch:
        await executemany(sql, batch)
