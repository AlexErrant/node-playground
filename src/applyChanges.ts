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
  changesets: readonly Changeset[]
) => {
  await transaction(async () => { // uncomment to make fail
    let maxVersion = 0n;
    // console.log("inserting changesets in tx", changesets);
    // TODO: may want to chunk
    try {
      // TODO: should we discard the changes altogether if they're less than the tracking version
      // we have for this peer?
      // that'd preclude resetting tho.
      for (const cs of changesets) {
        console.log("changeset", [cs[2], cs[3]])
        const v = BigInt(cs[4]);
        maxVersion = v > maxVersion ? v : maxVersion;
        // cannot use same statement in parallel
        await exec(JSON.stringify([cs[2], cs[3]]))
      }
    } catch (e) {
      console.error(e);
      throw e;
    } finally {
    }
  }); // uncomment to make fail
};

async function exec(input: string): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 1000))
  console.log("exec", input)
}

function transaction(cb: () => Promise<void>): Promise<void> {
  return serializeTx(async () => {
    await exec("BEGIN");
    try {
      await cb();
    } catch (e) {
      await exec("ROLLBACK");
      return;
    }
    await exec("COMMIT");
  });
}

function serializeTx(cb: () => any) {
  const res = txQueue.then(
    () => cb(),
    (e) => {
      console.error(e);
    }
  );
  txQueue = res;

  return res;
}

let txQueue: Promise<any> = Promise.resolve();