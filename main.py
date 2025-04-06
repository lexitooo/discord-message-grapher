from os import getenv
from dotenv import load_dotenv

load_dotenv()

from typing import Optional

import discord
from discord import app_commands

import asyncio

import csv

MY_GUILD = discord.Object(id=int(getenv("GUILD_ID")))  # replace with your guild id


class MyClient(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)


intents = discord.Intents.default()
intents.message_content = True
intents.guild_messages = True

client = MyClient(intents=intents)

csv_file = open(f"{getenv("GUILD_ID")}.csv", newline="", mode="w")

class CSVFields:
    DATETIME = "datetime"
    AUTHOR = "author"
    CHANNEL = "channel"
    CONTENT = "content"
    ID = "id"
    MENTIONS = "mentions"

def init_csv_dictwriter(csv_file) -> csv.DictWriter:
    fieldnames = [CSVFields.DATETIME,
                    CSVFields.AUTHOR,
                    CSVFields.CHANNEL,
                    CSVFields.CONTENT,
                    CSVFields.ID,
                    CSVFields.MENTIONS]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    return writer

csv_writer = init_csv_dictwriter(csv_file)

@client.event
async def on_ready():
    print(f'Logged in as {client.user} (ID: {client.user.id})')
    print('------')

def is_me():
        def predicate(interaction: discord.Interaction) -> bool:
            return interaction.user.id == int(getenv("OWNER_ID"))
        return app_commands.check(predicate)

@client.tree.command()
@app_commands.describe(
    channel='The channel you want to retrieve messages from.'
)
@is_me()
async def start_retrieval(interaction: discord.Interaction, channel: discord.TextChannel):
    asyncio.create_task(retrieve_messages(channel))
    await interaction.response.send_message(f'Message retrieval started.', ephemeral=True)

async def write_messages(messages: list[discord.Message]):
    for message in messages:
        csv_writer.writerow({CSVFields.DATETIME: message.created_at,
                                CSVFields.AUTHOR: message.author.id,
                                CSVFields.CHANNEL: message.channel.id,
                                CSVFields.CONTENT: message.content,
                                CSVFields.ID: message.id,
                                CSVFields.MENTIONS: "|".join([str(member.id) for member in message.mentions])})

async def retrieve_messages(channel: discord.TextChannel):
    counter = 0
    messages = []
    async for message in channel.history(limit=None):
        counter += 1

        messages.append(message)

        if counter % 500 == 0:
            print(f"Messages retrieved: {counter}")
            await write_messages(messages)
            messages = []
    await write_messages(messages)

client.run(getenv("TOKEN"))