import functools
import time


def _description_to_field_names(description):
  return [desc[0] for desc in description]


def _row_to_json(row, field_names):
  json_res = {}
  for i in range(len(row)):
      json_res[field_names[i]] = row[i]
  return json_res


def sql_to_json_results(cursor, sql):
  cursor.execute(sql)
  field_names = _description_to_field_names(cursor.description)
  json_results = []
  for row in cursor.fetchall():
      json_results.append(_row_to_json(row, field_names))
  return json_results


def poll(duration=1, backoff_multiplier=1, max_sleep=16):
    def decorator(func):
        @functools.wraps(func):
        def wrapper(*args, **kwargs):
            sleep_duration = min(duration, max_sleep)
            while True:
                success = func(*args, **kwargs)
                if success:
                    return success
                time.sleep(sleep_duration)
                sleep_duration = min(sleep_duration * backoff_multiplier, max_sleep)
        return wrapper
    return decorator
