SELECT clips.id,
       clips.client_id,
       path,
       REPLACE(sentence, '\r\n', ' ') AS sentence,
       COALESCE(SUM(votes.is_valid), 0) AS up_votes,
       COALESCE(SUM(NOT votes.is_valid), 0) AS down_votes,
       COALESCE(accents.accent, '') AS accent,
       locales.name AS locale
FROM clips
     LEFT JOIN votes ON clips.id = votes.clip_id
     LEFT JOIN user_clients ON clips.client_id = user_clients.client_id
     LEFT JOIN user_client_accents accents
               ON user_clients.client_id = accents.client_id AND
                  accents.locale_id = clips.locale_id
     LEFT JOIN locales ON clips.locale_id = locales.id
WHERE clips.locale_id in (2570,65,2578)
AND CREATED AT < "2020-03-12 00:00:00"
GROUP BY clips.id