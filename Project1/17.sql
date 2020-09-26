SELECT COUNT(*) AS '#Sangnok_Pokemon_spices'
FROM (SELECT DISTINCT P.name
      FROM Pokemon P, CatchedPokemon C, Trainer T
      WHERE T.id = C.owner_id
        AND T.hometown = 'Sangnok City'
        AND P.id = C.pid
     )A;

