SELECT P.id, E1.name AS 'Lv1', E2.name AS 'Lv2', E3.name AS 'LV3'
FROM Pokemon P,(SELECT before_id AS B, after_id AS A, P.name AS 'name'
                FROM Pokemon P, Evolution E
                WHERE P.id = E.before_id
               )E1, (SELECT before_id AS B, after_id AS A, P.name AS 'name'
                     FROM Pokemon P, Evolution E
                     WHERE P.id = E.before_id
                    )E2, (SELECT after_id AS A, P.name AS 'name'
                          FROM Pokemon P, Evolution E
                          WHERE P.id = E.after_id
                          )E3
WHERE E1.A = E2.B
  AND P.id = E1.B
  AND E2.A = E3.A
ORDER BY P.id;

