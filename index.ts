import knex = require("knex");
import assert = require("assert");
import { difference } from "lodash";
import Knex = require("knex");

const knexPg = knex({ client: "pg" });

/**
 * Returns a function that can be used to upsert rows into a given table.
 * The rows to upsert are passed as an array of objects. Note that only the
 * own, enumerable keys on the passed in objects are considered.
 *
 * If there are columns in the table that don't have a corresponding key in
 * _any_ of the passed in objects, then those columns are left as-is on update,
 * and given their default values (if any) on insert.
 *
 * If there are columns in the table for which _some but not all_ of the
 * objects passed in have a corresponding key, then this code can either:
 * 1) use the passed in value on the objects that have the key, and force the
 * column's value back to it's default -- meaning, NOT leave it as-is, even on
 * updates -- for those objects/rows that don't; or 2) throw. This behavior is
 * controlled by the `missingKeysBehavior` option.
 *
 * If a key is present in the input objects, but its value is undefined,
 * the default value is assigned (in insert and update).
 */
export default function <T extends string>(opts: {
  table: string;
  schema?: string;
  constraintCols: readonly T[];
  data: ({ [K in T]: any } & { [k: string]: any })[];
  onUpdateIgnore?: readonly string[];
  missingKeysBehavior?: "default" | "throw";
}) {
  const { table, schema, constraintCols, data } = opts;
  const keys = ownKeysUnion(data);
  const updateKeys = difference(keys, opts.onUpdateIgnore ?? constraintCols);

  assert(table, "Must provide a table name to upsert into.");
  assert(data.length, "Must provide some rows/objects to upsert.");
  assert(
    constraintCols.length,
    "Must provide some columns that uniquely identify existing rows to trigger an update."
  );

  if (opts.missingKeysBehavior === "throw") {
    for (const thisObj of data) {
      const thisObjKeys = Object.keys(thisObj);
      if (thisObjKeys.length !== keys.length) {
        throw new Error(
          "Not all objects shared the same (own, enumerable) keys. " +
            "In particular, some but not all objects had these keys: " +
            `${difference(keys, thisObjKeys).join(", ")}.`
        );
      }
    }
  }

  const insertTarget = schema
    ? knexPg.raw(`??.??`, [schema, table])
    : knexPg.ref(table);

  const insertValueLists = knexPg.raw(
    data
      .map(
        (it: any) =>
          `(${keys
            .map((k) => (it[k] === undefined ? "DEFAULT" : "?"))
            .join(",")})`
      )
      .join(","),
    data.flatMap((it) =>
      keys.map((k) => it[k as keyof typeof it]).filter((it) => it !== undefined)
    )
  );

  const insertQuery = knexPg.raw(`INSERT INTO ? ? VALUES ?`, [
    insertTarget,
    columnsList(keys, knexPg),
    insertValueLists,
  ]);

  const onConflictAction =
    updateKeys.length === 0
      ? knexPg.raw("DO NOTHING")
      : knexPg.raw(
          `DO UPDATE
          SET ${updateKeys.map((k) => `?? = EXCLUDED.??`).join(", ")}
          WHERE ${constraintCols
            .map((_colName) => `??.?? = EXCLUDED.??`)
            .join(" AND ")}`,
          updateKeys
            .flatMap((it) => [it, it])
            .concat(constraintCols.flatMap((it) => [table, it, it]))
        );

  const upsertQuery = knexPg.raw(`? ON CONFLICT ? ? RETURNING *`, [
    insertQuery,
    columnsList(constraintCols, knexPg),
    onConflictAction,
  ]);

  return rawToNative(upsertQuery);
}

function ownKeysUnion(objects: object[]) {
  return [...new Set(objects.flatMap(Object.keys))];
}

/**
 * work around for https://github.com/knex/knex/issues/3628.
 * Also includes the knex version of the query string, in case
 * you want to use it to recreate a knex.raw.
 */
function rawToNative(raw: Knex.Raw) {
  const { sql, bindings } = raw.toSQL();
  return {
    sql: (raw as any).client.positionBindings(sql),
    knexString: sql,
    bindings: (raw as any).client.prepBindings(bindings),
  };
}

function columnsList(columnNames: readonly string[], knex: Knex) {
  return knex.raw(
    `(${new Array(columnNames.length).fill("??").join(",")})`,
    columnNames
  );
}
