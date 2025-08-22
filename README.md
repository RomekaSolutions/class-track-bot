# class-track-bot
A Telegram bot to manage ESL class schedules, payments, cancellations, and student communications with a JSON-based data store.

## Admin Commands

- `/renewstudent <student_key> <num_classes> <YYYY-MM-DD>` – top up a student's class balance and set a new renewal date while preserving their schedule.

### Student identifiers

Many admin commands expect a `student_key`. Use the student's numeric Telegram ID if available; otherwise use the handle recorded when adding the student. After the student sends `/start` once, their numeric ID will work for future commands.
