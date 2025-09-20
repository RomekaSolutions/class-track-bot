import asyncio
import types

from test_admin_buttons import _setup as setup_admin_env


class DummyBot:
    def __init__(self):
        self.get_chat_calls = []
        self.edited_messages = []
        self.sent_messages = []

    async def get_chat(self, username):
        self.get_chat_calls.append(username)
        return types.SimpleNamespace(id=777, username="JohnDoe")

    async def edit_message_text(self, chat_id, message_id, text, reply_markup=None, **kwargs):
        self.edited_messages.append((chat_id, message_id, text, reply_markup))

    async def send_message(self, chat_id, text, reply_markup=None, **kwargs):
        self.sent_messages.append((chat_id, text, reply_markup))


class DummyJobQueue:
    def __init__(self):
        self.scheduled = []

    def jobs(self):
        return []

    def run_once(self, callback, when, name=None, data=None):
        self.scheduled.append((callback, when, name, data))


class DummyMessage:
    def __init__(self):
        self.chat = types.SimpleNamespace(id=555)
        self.message_id = 42
        self.replies = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append((text, reply_markup))


class DummyQuery:
    def __init__(self, data: str):
        self.data = data
        self.from_user = types.SimpleNamespace(id=123456789)
        self.message = DummyMessage()
        self.answered = None
        self.edited = None

    async def answer(self, text=None, show_alert=False):
        self.answered = (text, show_alert)

    async def edit_message_text(self, text, reply_markup=None, **kwargs):
        self.edited = (text, reply_markup)


class DummyIncomingMessage:
    def __init__(self, text: str):
        self.text = text
        self.replies = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append((text, reply_markup))


def _make_context(bot):
    job_queue = DummyJobQueue()
    application = types.SimpleNamespace(bot=bot, job_queue=job_queue)
    return types.SimpleNamespace(user_data={}, application=application, bot=bot)


def test_connect_offline_student_flow(monkeypatch, tmp_path):
    students = setup_admin_env(monkeypatch, tmp_path)
    # Reload module reference after setup to ensure patched functions are available
    import sys

    ctb = sys.modules["class_track_bot"]

    students.clear()
    students["john_doe"] = {
        "name": "John Doe",
        "class_dates": ["2099-12-31T10:00:00+07:00"],
        "classes_remaining": 5,
        "schedule_pattern": "",
        "cutoff_hours": 24,
        "cycle_weeks": 4,
        "class_duration_hours": 1.0,
        "free_class_credit": 0,
        "reschedule_credit": 0,
        "notes": [],
        "paused": False,
        "telegram_mode": False,
    }

    logs = ctb.load_logs()
    logs.clear()
    logs.append({"student": "john_doe", "status": "note"})

    def fake_save_logs(entries):
        snapshot = [dict(item) for item in entries]
        logs.clear()
        logs.extend(snapshot)

    monkeypatch.setattr(ctb, "save_logs", fake_save_logs)

    bot = DummyBot()
    context = _make_context(bot)

    query = DummyQuery("stu:CONNECT:john_doe")
    update = types.SimpleNamespace(callback_query=query)
    asyncio.run(ctb.connect_student_callback(update, context))

    assert "connect_student" in context.user_data
    assert "Telegram" in query.edited[0]

    admin_message = DummyIncomingMessage("@JohnDoe")
    admin_user = types.SimpleNamespace(id=123456789, username=None)
    update_message = types.SimpleNamespace(message=admin_message, effective_user=admin_user)

    asyncio.run(ctb.handle_message(update_message, context))

    assert "connect_student" not in context.user_data
    assert "john_doe" not in students
    assert "777" in students
    student = students["777"]
    assert student["telegram_mode"] is True
    assert student["telegram_id"] == 777
    assert student["telegram_handle"] == "johndoe"

    assert any("Connected John Doe" in reply[0] for reply in admin_message.replies)
    assert bot.edited_messages
    edited_text = bot.edited_messages[-1][2]
    assert "Student: John Doe" in edited_text

    assert logs[0]["student"] == "777"
    assert context.application.job_queue.scheduled
    assert bot.sent_messages
