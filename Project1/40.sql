SELECT A.hometown, A.nickname
FROM (SELECT hometown, nickname
      FROM Trainer T, CatchedPokemon C
      WHERE T.id = C.owner_id
      ORDER BY C.level DESC
      )A
GROUP BY A.hometown;

