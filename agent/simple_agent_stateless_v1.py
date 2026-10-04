import openai

import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

class SimpleAgentStatelessV1:
    def __init__(self):
        self.client = openai.OpenAI(api_key=OPENAI_API_KEY)

    async def make_prompt(self, user_message: str):
        return f"""
        You are a helpful assistant that can answer questions and help with tasks.
        The user has sent the following message: {user_message}
        Please answer the user's question or help with the task.
        """

    async def run(self, user_message: str):
        prompt = await self.make_prompt(user_message)
        response = await asyncio.to_thread(self.client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        print("simple agent stateless v1 got response")
        print(response.choices[0].message.content)
        return response.choices[0].message.content