SELECT T.name, COUNT(*) AS '#Pokemon'
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
  AND T.hometown = 'Sangnok City'
GROUP BY T.name
ORDER BY COUNT(*);

