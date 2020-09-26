SELECT A.name
FROM (SELECT T.name, COUNT(C.pid) AS 'Pokemon'
      FROM Trainer T, CatchedPokemon C
      WHERE T.id = C.owner_id
      GROUP BY T.name, C.pid
      )A
WHERE A.Pokemon>=2
ORDER BY A.name;

