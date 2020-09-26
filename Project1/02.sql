SELECT DISTINCT P.name
FROM (SELECT P.type
      FROM Pokemon P, 
      (SELECT COUNT(*) AS 'num' FROM Pokemon
                       GROUP BY type
                       ORDER BY COUNT(*) DESC LIMIT 1)C1,
      (SELECT type, COUNT(*) AS 'num' FROM Pokemon
       GROUP BY type)C2
      WHERE P.type = C2.type
      AND C1.num = C2.num)Fst,
     (SELECT P.type
      FROM Pokemon P,
      (SELECT MAX(C2.num) AS 'num'
       FROM (SELECT COUNT(*) AS 'num' FROM Pokemon
             GROUP BY type
             ORDER BY COUNT(*) DESC LIMIT 1)C1,
       (SELECT type, COUNT(*) AS 'num' FROM Pokemon
        GROUP BY type)C2
       WHERE C2.num < C1.num)S,
      (SELECT type, COUNT(*) AS 'num' FROM Pokemon
       GROUP BY type)C3
      WHERE P.type = C3.type
      AND C3.num = S.num)Snd,
      Pokemon P
WHERE P.type = Fst.type
OR P.type = Snd.type
ORDER BY P.name;

