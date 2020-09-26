SELECT T.id, C1.count AS '#Pokemon'
FROM Trainer T, (SELECT owner_id, COUNT(*) AS 'count'
                 FROM CatchedPokemon
                 GROUP BY owner_id
                )C1, (SELECT COUNT(*) AS 'count'
                      FROM CatchedPokemon
                      GROUP BY owner_id
                      ORDER BY COUNT(*) DESC LIMIT 1
                     )C2
WHERE T.id = C1.owner_id
  AND C1.count = C2.count
ORDER BY T.id;

