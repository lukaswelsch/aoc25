WITH data AS (
    SELECT
        string_split_regex(column0, '') AS arr,
        row_number() OVER () AS y
    FROM read_csv_auto('07/example.csv', header = false, delim = '')
),
grid AS (
	SELECT UNNEST(arr) AS val, generate_subscripts(arr, 1) as x,
	row_number() OVER () AS y
	FROM data
	WHERE NOT arr <@ ['.']
),
start AS (
    SELECT x, y
    FROM grid
    WHERE val = 'S'
),
recurse AS (
	WITH RECURSIVE beams (
		x,
		y
	) AS (
		--base case
		SELECT
			y + 1,
			val
		FROM start

		UNION ALL

		SELECT
			b.y + 1 AS y,
			b.val
		FROM beams b
		JOIN grid g ON g.y = b.y AND
	)


)
FROM grid



--,
--recurse AS (
--	WITH RECURSIVE tachyon (
--		x,
--        y,
--        cell,
--        RecursionStep
--    ) AS (
--        -- Base Case
--        SELECT
--            x,
--            y,
--
--
--    )
--
--
--)

