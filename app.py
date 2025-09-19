import asyncio
import logging
import sys
import os

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from slack.bot import SlackBot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    """Main function to run the Slack bot"""
    bot = SlackBot()
    
    try:
        await bot.start()
        # Keep the bot running
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logging.error(f"Error running bot: {e}")
    finally:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())