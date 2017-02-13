import { Processor, ProcessorFunction, ProcessorContext, CmdPacket, msgpack_encode } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient} from "redis";
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

processor.callAsync("refresh", async (ctx: ProcessorContext) => {
  log.info("refresh");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;

  try {
    const result = await db.query("SELECT p.id AS p_id, p.title AS p_title, p.description AS p_description, p.image AS p_image, p.thumbnail AS p_thumbnail, p.period AS p_period, p.show_in_index AS p_show_in_index, pr.id AS pr_id, pr.name AS pr_name, pr.title AS pr_title, pr.description AS pr_description FROM plans AS p LEFT JOIN plan_rules AS pr ON p.id = pr.pid", []);
    const plans = [];
    let last_pid = null;
    let plan = null;
    for (const row of result.rows) {
      if (row.p_id !== last_pid) {
        plan = {
          id: row.p_id,
          title: row.p_title ? row.p_title.trim() : "",
          description: row.p_description,
          image: row.p_image ? row.p_image.trim() : "",
          thumbnail: row.p_thumbnail ? row.p_thumbnail.trim() : "",
          period: row.p_period,
          show_in_index: row.p_show_in_index,
          rules: [],
          items: []
        };
        plans.push (plan);
        last_pid = plan.id;
      }
      if (plan != null) {
        if (row.pr_id != null) {
          const rule = {
            id: row.pr_id,
            name: row.pr_name ? row.pr_name.trim() : "",
            title: row.pr_title ? row.pr_title.trim() : "",
            description: row.pr_description
          };
          plan.rules.push(rule);
        }
      }
    }
    for (const plan of plans) {
      const result1 = await db.query("SELECT id, title, description FROM plan_items WHERE pid = $1", [ plan.id ])
      for (const row of result1.rows) {
        plan.items.push(row2item(row));
      }
    }
    const multi = cache.multi();
    for (const plan of plans) {
      const buf = await msgpack_encode(plan);
      delete plan["items"];
      delete plan["rules"];
      const slimbuf = await msgpack_encode(plan);
      multi.hset("plan-entities", plan["id"], buf);
      multi.hset("plan-slim-entities", plan["id"], slimbuf);
    }
    for (const plan of plans) {
      multi.sadd("plans", plan["id"]);
    }
    multi.exec((err2, replies) => {
      if (err2) {
        log.error(err2);
      }
      done(); // close db and cache connection
    });
  } catch (e) {
    done();
    log.error(e);
  }
});

function row2item(row) {
  return {
    id: row.id,
    title: row.title ? row.title.trim() : "",
    description: row.description
  };
}
