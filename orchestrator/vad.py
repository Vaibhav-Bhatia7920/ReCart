import asyncio
import numpy as np
import torch

# 1. Initialize Silero VAD using PyTorch Hub
# This fetches the model and provides utility classes.
model, utils = torch.hub.load(
    repo_or_dir='snakers4/silero-vad',
    model='silero_vad',
    force_reload=False
)
(get_speech_timestamps, save_audio, read_audio, VADIterator, collect_chunks) = utils

# 2. Configuration Parameters
SAMPLE_RATE = 16000  # Silero loves 16000Hz (or 8000Hz)
WINDOW_SIZE_SAMPLES = 512  # 512 samples @ 16kHz = exactly 32ms of audio
THRESHOLD = 0.5  # Sensitivity threshold (0.0 to 1.0)

# 3. Setup the Iterator
# VADIterator manages the internal state machine (tracking state changes seamlessly)
vad_iterator = VADIterator(
    model, 
    threshold=THRESHOLD, 
    sampling_rate=SAMPLE_RATE, 
    min_silence_duration_ms=300, # Dictates delay before declaring speech has ended
    speech_pad_ms=100            # Buffers the beginning/end edges of speech
)

async def audio_stream_generator():
    """
    Mocking your raw audio stream. 
    In production, replace this with your actual audio bytes source 
    (e.g., websocket.recv(), pyaudio stream, or a custom WebRTC track).
    """
    while True:
        # Silero expects 16-bit PCM Audio chunks matching the WINDOW_SIZE_SAMPLES (512 samples)
        # Yielding 512 samples of empty/dummy bytes every 32ms
        dummy_pcm_bytes = b'\x00' * (WINDOW_SIZE_SAMPLES * 2) 
        yield dummy_pcm_bytes
        await asyncio.sleep(0.032) 

async def main():
    print("🚀 Starting standalone Silero VAD loop...")
    
    async for raw_pcm_chunk in audio_stream_generator():
        # Convert raw PCM bytes (int16) to a float32 numpy array, then to a PyTorch Tensor
        audio_int16 = np.frombuffer(raw_pcm_chunk, dtype=np.int16)
        audio_float32 = audio_int16.astype(np.float32) / 32768.0
        tensor_chunk = torch.from_tensor(audio_float32) if hasattr(torch, 'from_tensor') else torch.from_numpy(audio_float32)
        
        # Pass the 32ms chunk into the iterator
        # vad_iterator internally updates state and returns a dict if a state boundary is hit
        speech_dict = vad_iterator(tensor_chunk, return_seconds=True)
        
        if speech_dict:
            if "start" in speech_dict:
                print(f"🎙️ Speech STARTED at {speech_dict['start']:.2f}s -> (Stop agent TTS / Start buffering for STT)")
                # TODO: Trigger logic to interrupt your LLM/TTS engine here
                
            elif "end" in speech_dict:
                print(f"🤫 Speech ENDED at {speech_dict['end']:.2f}s -> (Send collected buffer to Deepgram/STT)")