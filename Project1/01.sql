SELECT A.name
FROM (SELECT T.name, C.owner_id
      FROM Trainer T, CatchedPokemon C
      WHERE T.id = C.owner_id
     )A
GROUP BY A.owner_id
HAVING COUNT(A.owner_id)>=3
ORDER BY COUNT(*) DESC;
