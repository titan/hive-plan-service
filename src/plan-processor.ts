import { Processor, ProcessorFunction, ProcessorContext, CmdPacket, async_serial_ignore } from "hive-service";
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

processor.call("refresh", (ctx: ProcessorContext) => {
  log.info("refresh");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  db.query("SELECT p.id AS p_id, p.title AS p_title, p.description AS p_description, p.image AS p_image, p.thumbnail AS p_thumbnail, p.period AS p_period, p.show_in_index AS p_show_in_index, pr.id AS pr_id, pr.name AS pr_name, pr.title AS pr_title, pr.description AS pr_description FROM plans AS p LEFT JOIN plan_rules AS pr ON p.id = pr.pid", [], (err: Error, result) => {
    if (err) {
      log.error(err, "query error");
      done();
      return;
    }
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
    const ps = plans.map(plan => {
      return new Promise<Object>((resolve, reject) => {
        db.query("SELECT id, title, description FROM plan_items WHERE pid = $1", [ plan.id ], (err1: Error, result1) => {
          if (err1) {
            reject(err1);
          } else {
            for (const row of result1.rows) {
              plan.items.push(row2item(row));
            }
            resolve(plan);
          }
        });
      });
    });
    async_serial_ignore<Object>(ps, (plans) => {
      const multi = cache.multi();
      for (const plan of plans) {
        multi.hset("plan-entities", plan["id"], JSON.stringify(plan));
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
    });
  });
});

function row2item(row) {
  return {
    id: row.id,
    title: row.title ? row.title.trim() : "",
    description: row.description
  };
}
