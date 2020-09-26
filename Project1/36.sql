SELECT T.name
FROM Trainer T, (SELECT after_id
                 FROM Pokemon, Evolution
                 WHERE after_id <> ALL (
                   SELECT before_id
                   FROM Evolution
                   )
                 )A, CatchedPokemon C
WHERE T.id = C.owner_id
  AND C.pid = A.after_id
GROUP BY T.name
ORDER BY T.name;
