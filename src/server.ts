import { Server, Config, Context, ResponseFunction, Permission } from "hive-server";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as bunyan from "bunyan";
import { servermap } from "hive-hostmap";
import { verify, uuidVerifier, arrayVerifier } from "hive-verify";

let log = bunyan.createLogger({
  name: "plan-server",
  streams: [
    {
      level: "info",
      path: "/var/log/plan-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/plan-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let list_key = "plans";
let entities_prefix = "plans-";
let entity_key = "plan-entities";

let config: Config = {
  svraddr: servermap["plan"],
  msgaddr: "ipc:///tmp/plan.ipc",
  cacheaddr: process.env["CACHE_HOST"]
};

let svr = new Server(config);

let allowall: Permission[] = [["mobile", true], ["admin", true]];

svr.call("getAvailablePlans", allowall, (ctx: Context, rep: ResponseFunction) => {
  log.info("getAvailablePlans uid: %s", ctx.uid);
  // http://redis.io/commands/sdiff
  ctx.cache.sdiff(list_key, entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2plans(ctx, result, rep);
    }
  });
});

svr.call("getJoinedPlans", allowall, (ctx: Context, rep: ResponseFunction) => {
  log.info("getJoinedPlans uid: %s", ctx.uid);
  // http://redis.io/commands/smembers
  ctx.cache.smembers(entities_prefix + ctx.uid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      ids2plans(ctx, result, rep);
    }
  });
});

svr.call("getPlan", allowall, (ctx: Context, rep: ResponseFunction, pid: string) => {
  log.info("getPlan uid: %s, pid: %s", ctx.uid, pid);
  if (!verify([uuidVerifier("pid", pid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  // http://redis.io/commands/hget
  ctx.cache.hget(entity_key, pid, (err, planstr) => {
    if (err) {
      rep({
        code: 500,
        msg: err.message
      });
    } else if (planstr) {
      let plan = JSON.parse(planstr);
      ctx.cache.hget("plan-joined-count", plan.id, (err, count) => {
        plan.joinedCount = count ? count : 0;
        rep(plan);
      });
    } else {
      rep({
        code: 404,
        msg: "Plan not found"
      });
    }
  });
});

svr.call("increaseJoinedCount", allowall, (ctx: Context, rep: ResponseFunction, pid: string) => {
  log.info("increaseJoinedCount uid: %s, pid: %s", ctx.uid, pid);
  if (!verify([uuidVerifier("pid", pid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hincrby("plan-joined-count", pid, 1, (err, count) => {
    if (err) {
      rep({
        code: 500,
        msg: err
      });
    } else {
      rep({ count });
    }
  });
});

svr.call("decreaseJoinedCount", allowall, (ctx: Context, rep: ResponseFunction, pid: string) => {
  log.info("decreaseJoinedCount uid: %s, pid: %s", ctx.uid, pid);
  if (!verify([uuidVerifier("pid", pid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hincrby("plan-joined-count", pid, -1, (err, count) => {
    if (err) {
      rep({
        code: 500,
        msg: err
      });
    } else {
      rep({ count });
    }
  });
});

svr.call("setJoinedCounts", allowall, (ctx: Context, rep: ResponseFunction, params: [string, number][]) => {
  log.info("setJoinedCounts uid: %s, params: %j", ctx.uid, params);
  if (!verify([arrayVerifier("params", params)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let multi = ctx.cache.multi();
  for (let [pid, count] of params) {
    multi.hset("plan-joined-count", pid, count);
  }
  multi.exec((err, replies) => {
    if (err) {
      rep({
        code: 500,
        msg: err
      });
    } else {
      rep({
        code: 200,
        msg: "SUCCESS"
      });
    }
  });
});

svr.call("refresh", allowall, (ctx: Context, rep: ResponseFunction) => {
  log.info("refresh uid: %s", ctx.uid);
  ctx.msgqueue.send(msgpack.encode({cmd: "refresh", args: null}));
  rep({status: "okay"});
});

function ids2plans(ctx: Context, ids: string[], rep: ResponseFunction) {
  let multi = ctx.cache.multi();
  for (let id of ids) {
    multi.hget(entity_key, id);
  }
  multi.exec(function(err, planstrs) {
    if (err) {
      rep([]);
    } else {
      let plans = [];
      for (let planstr of planstrs) {
        let plan = JSON.parse(planstr);
        multi.hget("plan-joined-count", plan.id);
        plans.push(plan);
      }
      multi.exec((err, counts) => {
        for (let i in counts) {
          let p = plans[i];
          p.joined_count = counts[i] ? counts[i] : 0;
        }
        rep(plans);
      });
    }
  });
}

log.info("Start server at %s and connect to %s", config.svraddr, config.msgaddr);

svr.run();
