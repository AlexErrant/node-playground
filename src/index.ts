import aio from "@vlcn.io/crsqlite-allinone"
import { Changeset, changesReceived } from "./applyChanges.js"

export const initSql = [
  `CREATE TABLE IF NOT EXISTS myTable (
    id BLOB PRIMARY KEY,
    a,
    b,
    c,
    d,
    e,
    f,
    g,
    h
);`,
  `SELECT crsql_as_crr('myTable');`,
]

const dbSource = aio.open()
dbSource.execMany(initSql)
dbSource.exec(
  `INSERT INTO myTable (id,a,b,c,d,e,f,g,h)
                  VALUES (?,?,?,?,?,?,?,?,?)`,
  [
    "A7A33CBF-65DD-4D36-B193-E64B9EC61EC7",
    "a value",
    "b value",
    "c value",
    "d value",
    "e value",
    "f value",
    "g value",
    "h value",
  ]
)

const changes: Changeset[] = await dbSource.execA<Changeset>(
  `SELECT "table", "pk", "cid", "val", "version", "site_id" FROM crsql_changes`
)

const dbTarget = aio.open()
dbTarget.execMany(initSql)

try {
  await changesReceived(changes)
} finally {
  console.log("closing db")
  dbTarget.close()
}

console.log("The end! yay")
