SELECT name, COUNT(DISTINCT client_id) AS count
  FROM clips
  LEFT JOIN locales ON clips.locale_id = locales.id
  GROUP BY locale_id