import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple


class TurnMetricsParser:
    def __init__(self):
        # Maps (call_id, turn_id) -> dict of event timestamps
        self.turns: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(dict)

    def ingest_log_line(self, line: str) -> None:
        """Parses a single JSON log string and records the timestamp."""
        line = line.strip()
        if not line:
            return

        record = json.loads(line)
        call_id = str(record.get("call_id"))
        turn_id = str(record.get("turn_id"))
        event = record.get("event")
        timestamp = float(record.get("time_stamp"))

        if call_id and turn_id and event:
            self.turns[(call_id, turn_id)][event] = timestamp

    def ingest_logs(self, log_lines: List[str]) -> None:
        """Parses multiple log lines."""
        for line in log_lines:
            self.ingest_log_line(line)

    def get_metrics(self, call_id: str, turn_id: str) -> Optional[Dict[str, float]]:
        """
        Calculates and returns stage latencies (in seconds and milliseconds)
        for a given (call_id, turn_id).
        """
        key = (str(call_id), str(turn_id))
        if key not in self.turns:
            return None

        events = self.turns[key]

        t_first_asr = events.get("First ASR Transcript")
        t_first_agent = events.get("First Agent Transcript")
        t_last_asr = events.get("Last ASR Transcript")
        t_last_agent = events.get("Last Agent Transcript")
        t_last_audio = events.get("Last Audio Frame")

        metrics = {
            # Time from STT receiving audio / first partial to Agent yielding first token
            "time_to_first_agent_chunk_sec": (
                round(t_first_agent - t_first_asr, 4)
                if t_first_agent and t_first_asr
                else None
            ),
            # Full duration of STT transcription
            "stt_duration_sec": (
                round(t_last_asr - t_first_asr, 4)
                if t_last_asr and t_first_asr
                else None
            ),
            # Agent processing/generation window (from first token to completion)
            "agent_generation_duration_sec": (
                round(t_last_agent - t_first_agent, 4)
                if t_last_agent and t_first_agent
                else None
            ),
            # Synthesis/Playback completion lag after agent finished outputting tokens
            "tts_trailing_duration_sec": (
                round(t_last_audio - t_last_agent, 4)
                if t_last_audio and t_last_agent
                else None
            ),
            # End-to-end turn duration (from first ASR frame to last audio emitted)
            "e2e_turn_duration_sec": (
                round(t_last_audio - t_first_asr, 4)
                if t_last_audio and t_first_asr
                else None
            ),
        }

        # Millisecond representation
        metrics_ms = {
            f"{k.replace('_sec', '_ms')}": (
                round(v * 1000, 2) if v is not None else None
            )
            for k, v in metrics.items()
        }

        return {
            "call_id": str(call_id),
            "turn_id": str(turn_id),
            "raw_events": events,
            **metrics,
            **metrics_ms,
        }