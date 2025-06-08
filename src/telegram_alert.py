import requests

class TelegramBot:
    """
    Simple Telegram Bot to send messages to a specific chat.

    Usage:
        from telegram_alert import TelegramBot

        bot = TelegramBot(
            token="YOUR_BOT_TOKEN",
            chat_id=123456789
        )
        bot.send_message("Hello from class-based Telegram bot!")
    """
    def __init__(self, token: str, chat_id: int, timeout: int = 10):
        """
        Initialize the TelegramBot instance.

        :param token: Bot token from BotFather.
        :param chat_id: Numeric chat ID to send messages to.
        :param timeout: Request timeout in seconds (default: 10).
        """
        self.token = token
        self.chat_id = chat_id
        self.timeout = timeout
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    def send_message(self, text: str) -> bool:
        """
        Send a text message via the Telegram Bot API.

        :param text: Message text to send.
        :return: True if the message was sent successfully, False otherwise.
        """
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text
        }
        try:
            response = requests.post(url, data=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            return result.get("ok", False)
        except Exception as e:
            print(f"[TelegramBot] Error sending message: {e}")
            return False


if __name__ == "__main__":
    # Example usage: load real values *only* from environment
    import os
    TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Please set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in your environment")

    bot = TelegramBot(token=TELEGRAM_TOKEN, chat_id=int(TELEGRAM_CHAT_ID))
    bot.send_message("🚀 Test alert from class-based bot!")

