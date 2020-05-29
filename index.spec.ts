import sut from "./index";

test("should error for empty data", () => {
  expect(() =>
    sut({
      table: "x",
      data: [],
      constraintCols: ["id"],
    })
  ).toThrowError();
});

test("should error for empty constraints", () => {
  expect(() =>
    sut({
      table: "x",
      data: [{ id: 4 }],
      constraintCols: [],
    })
  ).toThrowError();
});

test("should not require a schema", () => {
  assertResMatches(
    sut({
      table: "x",
      constraintCols: ["id"],
      data: [{ id: 1, other: 2 }],
    }),
    `INSERT INTO "x" ("id","other")
      VALUES ($1,$2)
      ON CONFLICT ("id") DO UPDATE
        SET "other" = EXCLUDED."other"
        WHERE "x"."id" = EXCLUDED."id"
      RETURNING *`,
    [1, 2]
  );
});

test("should work with a schema", () => {
  assertResMatches(
    sut({
      table: "x",
      schema: "y",
      constraintCols: ["id"],
      data: [{ id: 1, other: 2 }],
    }),
    `INSERT INTO "y"."x" ("id","other")
      VALUES ($1,$2)
      ON CONFLICT ("id") DO UPDATE
        SET "other" = EXCLUDED."other"
        WHERE "x"."id" = EXCLUDED."id"
      RETURNING *`,
    [1, 2]
  );
});

test("should support multiple objects", () => {
  assertResMatches(
    sut({
      table: "x",
      schema: "y",
      constraintCols: ["id"],
      data: [
        { id: 1, other: 4 },
        { id: 2, other: "hello" },
      ],
    }),
    `INSERT INTO "y"."x" ("id","other")
      VALUES ($1,$2),($3,$4)
      ON CONFLICT ("id") DO UPDATE
        SET "other" = EXCLUDED."other"
        WHERE "x"."id" = EXCLUDED."id"
      RETURNING *`,
    [1, 4, 2, "hello"]
  );
});

test("should support using default on some undefined/missing values", () => {
  const baseOpts = {
    table: "x",
    schema: "y",
    constraintCols: ["id"],
  };

  // Test two similar cases, which should be treated identically
  const [res1, res2] = [
    sut({
      ...baseOpts,
      data: [{ id: 1 }, { id: 2, other: "hello" }],
    }),
    sut({
      ...baseOpts,
      data: [
        { id: 1, other: undefined },
        { id: 2, other: "hello" },
      ],
    }),
  ];

  expect(res1).toEqual(res2);
  assertResMatches(
    res1,
    `INSERT INTO "y"."x" ("id","other")
      VALUES ($1,DEFAULT),($2,$3)
      ON CONFLICT ("id") DO UPDATE
        SET "other" = EXCLUDED."other"
        WHERE "x"."id" = EXCLUDED."id"
      RETURNING *`,
    [1, 2, "hello"]
  );
});

test("should support throwing upon finding some missing values", () => {
  const baseOpts = {
    table: "x",
    schema: "y",
    constraintCols: ["id"],
    missingKeysBehavior: <const>"throw",
  };

  expect(() =>
    sut({
      ...baseOpts,
      data: [{ id: 1 }, { id: 2, other: "hello" }],
    })
  ).toThrowErrorMatchingInlineSnapshot(
    `"Not all objects shared the same (own, enumerable) keys. In particular, some but not all objects had these keys: other."`
  );

  // This call should _not_throw though, because explicit undefined
  // triggers default, as documented.
  sut({
    ...baseOpts,
    data: [
      { id: 1, other: undefined },
      { id: 2, other: "hello" },
    ],
  });
});

function assertResMatches(
  res: ReturnType<typeof sut>,
  sql: string,
  bindings: any[]
) {
  expect(res.bindings).toEqual(bindings);
  expect(normalize(res.sql)).toEqual(normalize(sql));
  expect(res.sql.replace(/\$\d+/g, "?")).toEqual(res.knexString);
}

function normalize(sql: string) {
  return sql.replace(/\s+/g, " ").toLowerCase().trim();
}
