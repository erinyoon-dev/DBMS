SELECT A.type
FROM (SELECT P.id AS 'id', P.type AS 'type'
      FROM Pokemon P, Evolution E
      WHERE P.id = E.before_id
     )A
GROUP BY A.type
HAVING COUNT(A.id)>=3
ORDER BY A.type DESC;

