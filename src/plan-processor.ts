import { Processor, ProcessorFunction, ProcessorContext, CmdPacket, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient, Multi } from "redis";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";

export const processor = new Processor();

const log = bunyan.createLogger({
  name: "plan-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/plan-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/plan-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

async function refresh_plans(db: PGClient, cache: RedisClient) {
  const result = await db.query("SELECT id, title, description, optional from plans");
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const row of result.rows) {
    const plan = {
      id: row.id,
      title: row.title,
      description: row.description,
      optional: row.optional,
    };
    const pkt = await msgpack_encode_async(plan);
    multi.hset("plan-entities", plan.id + "", pkt);
  }
  return multi.execAsync();
}

async function refresh_plan_groups(db: PGClient, cache: RedisClient) {
  const result = await db.query("SELECT id, title, mask from plan_groups");
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (const row of result.rows) {
    const group = {
      id: row.id,
      title: row.title,
      description: row.description,
      mask: row.mask,
      plans: [],
    };

    for (let i = 0; i < 64; i ++) {
      if ((group.mask & (1 << i)) > 0) {
        const id = (1 << i) + "";
        const pkt = await cache.hgetAsync("plan-entities", id);
        if (pkt) {
          const plan = await msgpack_decode_async(pkt);
          if (plan) {
            group.plans.push(plan);
          }
        }
      }
    }
    const pkt = await msgpack_encode_async(group);
    multi.hset("plan-group-entities", group.id, pkt);
  }
  return multi.execAsync();
}

processor.callAsync("refresh", async (ctx: ProcessorContext) => {
  log.info("refresh");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;

  try {
    await refresh_plans(db, cache);
    await refresh_plan_groups(db, cache);
    return { code: 200, data: "okay" };
  } catch (e) {
    log.error(e);
    return { code: 500, msg: e.message };
  }
});
