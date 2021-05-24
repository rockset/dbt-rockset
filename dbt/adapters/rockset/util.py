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
        