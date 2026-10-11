from orchestrator.providers import database_orchestrate, speaker_orchestrate
from pathlib import Path
import asyncio
from telemetry.logger import Logger

async def latency_orchestrate(call_id: str, turn_id: str):
    path = Path(__file__).parent.parent / "audio_db" / "audio_files" / "human-nonhuman" / "human" 
    call_id = str('123')
    turn_id = str('456')
    logger = Logger(call_id)
    await database_orchestrate(call_id, turn_id, path, logger)


if __name__ == "__main__":
    asyncio.run(latency_orchestrate("123", "456"))