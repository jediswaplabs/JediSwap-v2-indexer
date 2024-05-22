import json
import aiohttp
import redis
import os

REDIS_URL = os.getenv('PERSIST_TO_REDIS', 'redis://localhost')
STRK_GRANT_URL = "https://kx58j6x5me.execute-api.us-east-1.amazonaws.com/starknet/fetchFile?file=strk_grant.json"

async def fetch_strk_grant_data():
    r = redis.Redis.from_url(REDIS_URL)
    cached_data = r.get('Jediswap_V1')
    
    if cached_data:
        jediswap_data = json.loads(cached_data)
    else:
        async with aiohttp.ClientSession() as session:
            async with session.get(STRK_GRANT_URL) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    jediswap_data = data.get('Jediswap_v1', {})
                    r.set('Jediswap_V1', json.dumps(jediswap_data), ex=3600)  # Cache for 1 hour
                else:
                    return {"error": "Failed to fetch data"}
    for key, value in jediswap_data.items():
        if len(value) > 0:
            jediswap_data[key] = value[-1]
        else:
            jediswap_data[key] = {}
    return jediswap_data


