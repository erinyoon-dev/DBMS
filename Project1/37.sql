SELECT P.name, MAX(P.lv) AS 'TotalLv'
FROM (SELECT SUM(C.level) AS 'lv', T.name
      FROM CatchedPokemon C, Trainer T
      WHERE C.owner_id = T.id
      GROUP BY T.name
      ORDER BY SUM(C.level) DESC
     )P;

