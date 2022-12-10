import { DB, DBAsync } from "@vlcn.io/xplat-api"

export type SiteIDWire = string
export type SiteIDLocal = Uint8Array
type CID = string
type QuoteConcatedPKs = string | number
type TableName = string
type Version = number | string

export type Changeset = [
  TableName,
  QuoteConcatedPKs,
  CID,
  any, // val,
  Version,
  SiteIDWire // site_id
]

// if we fail to apply, re-request
// TODO: other retry mechanisms
// todo: need to know who received from. cs site id can be a forwarded site id
export const changesReceived = async (
  fromSiteId: SiteIDWire,
  changesets: readonly Changeset[]
) => {
  await this.db.transaction(async () => {
    let maxVersion = 0n;
    log("inserting changesets in tx", changesets);
    const stmt = await this.db.prepare(
      'INSERT INTO crsql_changes ("table", "pk", "cid", "val", "version", "site_id") VALUES (?, ?, ?, ?, ?, ?)'
    );
    // TODO: may want to chunk
    try {
      // TODO: should we discard the changes altogether if they're less than the tracking version
      // we have for this peer?
      // that'd preclude resetting tho.
      for (const cs of changesets) {
        const v = BigInt(cs[4]);
        maxVersion = v > maxVersion ? v : maxVersion;
        // cannot use same statement in parallel
        await stmt.run(
          cs[0],
          cs[1],
          cs[2],
          cs[3],
          v,
          cs[5] ? uuidParse(cs[5]) : 0
        );
      }
    } catch (e) {
      console.error(e);
      throw e;
    } finally {
      stmt.finalize();
    }
    await this.db.exec(
      `INSERT OR REPLACE INTO __crsql_wdbreplicator_peers (site_id, version) VALUES (?, ?)`,
      [uuidParse(fromSiteId), maxVersion]
    );
  });
};