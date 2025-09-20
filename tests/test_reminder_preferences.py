from datetime import timedelta
from types import SimpleNamespace

import class_track_bot as bot


class DummyJob:
    def __init__(self, name: str):
        self.name = name
        self.removed = False

    def schedule_removal(self) -> None:
        self.removed = True


class DummyJobQueue:
    def __init__(self):
        self._jobs = []
        self.run_once_calls = []

    def add_job(self, name: str) -> DummyJob:
        job = DummyJob(name)
        self._jobs.append(job)
        return job

    def jobs(self):
        return list(self._jobs)

    def run_once(self, callback, when, name, data):
        self.run_once_calls.append(
            SimpleNamespace(callback=callback, when=when, name=name, data=data)
        )


def test_normalize_students_sets_default_reminder():
    students = {
        "1": {
            "name": "Alice",
            "telegram_id": 1,
            "cutoff_hours": bot.DEFAULT_CUTOFF_HOURS,
            "cycle_weeks": bot.DEFAULT_CYCLE_WEEKS,
            "class_duration_hours": bot.DEFAULT_DURATION_HOURS,
        }
    }

    changed = bot.normalize_students(students)

    assert changed is True
    assert students["1"]["reminder_offset_minutes"] == bot.DEFAULT_REMINDER_MINUTES


def test_normalize_students_casts_string_preference():
    students = {
        "1": {
            "name": "Alice",
            "telegram_id": 1,
            "cutoff_hours": bot.DEFAULT_CUTOFF_HOURS,
            "cycle_weeks": bot.DEFAULT_CYCLE_WEEKS,
            "class_duration_hours": bot.DEFAULT_DURATION_HOURS,
            "reminder_offset_minutes": "30",
        }
    }

    bot.normalize_students(students)

    assert students["1"]["reminder_offset_minutes"] == 30


def test_schedule_student_reminders_disabled_skips_jobs():
    queue = DummyJobQueue()
    existing = queue.add_job("class_reminder:1:old")
    application = SimpleNamespace(job_queue=queue)
    student = {
        "name": "Alice",
        "telegram_id": 1,
        "telegram_mode": True,
        "reminder_offset_minutes": 0,
        "class_dates": ["2999-01-01T12:00:00+07:00"],
        "cancelled_dates": [],
    }

    bot.schedule_student_reminders(application, "1", student)

    assert existing.removed is True
    assert queue.run_once_calls == []


def test_schedule_student_reminders_respects_preference():
    queue = DummyJobQueue()
    application = SimpleNamespace(job_queue=queue)
    dt_str = "2999-01-01T12:00:00+07:00"
    student = {
        "name": "Alice",
        "telegram_id": 1,
        "telegram_mode": True,
        "reminder_offset_minutes": 30,
        "class_dates": [dt_str],
        "cancelled_dates": [],
    }

    bot.schedule_student_reminders(application, "1", student)

    assert len(queue.run_once_calls) == 1
    call = queue.run_once_calls[0]
    assert call.name == f"class_reminder:1:{dt_str}"
    assert call.data == {"student_key": "1", "class_dt": dt_str}
    class_dt = bot.ensure_bangkok(dt_str)
    assert call.when == class_dt - timedelta(minutes=30)


def test_start_message_shows_notification_summary():
    dt_str = "2999-01-01T12:00:00+07:00"
    student = {
        "name": "Alice",
        "telegram_id": 1,
        "telegram_mode": True,
        "reminder_offset_minutes": 15,
        "class_dates": [dt_str],
        "cancelled_dates": [],
        "classes_remaining": 3,
        "free_class_credit": 0,
        "paused": False,
        "premium": False,
    }

    text, markup = bot.build_start_message(student)

    assert "Reminder notifications: 15 minutes before class" in text
    button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
    assert "🔔 Notification Settings" in button_texts
