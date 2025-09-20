# class-track-bot
A Telegram bot to manage ESL class schedules, payments, cancellations, and student communications with a JSON-based data store.

## Admin Commands

- `/renewstudent <student_key> <num_classes> <YYYY-MM-DD>` – top up a student's class balance and set a new renewal date while preserving their schedule.
- `/liststudents` – show all active students with their Telegram handles or IDs.

### Student identifiers

Many admin commands expect a `student_key`. Use the student's numeric Telegram ID if available; otherwise use the handle recorded when adding the student. After the student sends `/start` once, their numeric ID will work for future commands.

## Getting Started

1. **Install dependencies**

   This project relies on `python-telegram-bot` and `pytz`. Install them with:

   ```bash
   pip install python-telegram-bot pytz
   ```

2. **Configure environment variables**

   Set your bot token and admin IDs before running the bot:

   ```bash
   export TELEGRAM_BOT_TOKEN="<your-telegram-bot-token>"
   export ADMIN_IDS="123456789,987654321"  # comma-separated user IDs
   ```

3. **Run the bot**

   ```bash
   python class_track_bot.py
   ```

## Development

### Running tests

Run the test suite with [pytest](https://docs.pytest.org/):

```bash
pytest
```
