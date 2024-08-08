import os
import re
import shutil
import sqlite3
import threading
import difflib
import asyncio
import aiohttp
from colorama import init, Fore, Style

def process_unaccounted_folder(dir_path, dest_dir):
    print(f"Here we are! dir:{dir_path} dest:{dest_dir")
