import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response } from "hive-service";
import * as bunyan from "bunyan";
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

let allowall: Permission[] = [["mobile", true], ["admin", true]];

export const server = new Server();

server.call("getAvailablePlans", allowall, "获取可用计划", "获取当前用户可用的计划，如果没有指定 uid，则得到全部计划", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("getAvailablePlans uid: %s", ctx.uid);
  ctx.cache.sdiff("plans", `plans-of-user:${ctx.uid}`, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      ids2plans(ctx, result, rep);
    }
  });
});

server.call("getJoinedPlans", allowall, "得到用户已加入的计划", "得到当前用户已加入的计划", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("getJoinedPlans uid: %s", ctx.uid);
  ctx.cache.smembers(`plans-of-user:${ctx.uid}`, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else {
      ids2plans(ctx, result, rep);
    }
  });
});

server.call("getPlan", allowall, "获得计划详情", "获得计划详情，包括已加入的车辆数。", (ctx: ServerContext, rep: ((result: any) => void), pid: string) => {
  log.info("getPlan uid: %s, pid: %s", ctx.uid, pid);
  if (!verify([uuidVerifier("pid", pid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("plan-entities", pid, (err, planstr) => {
    if (err) {
      rep({
        code: 500,
        msg: err.message
      });
    } else if (planstr) {
      let plan = JSON.parse(planstr);
      ctx.cache.hget("plan-joined-count", plan.id, (err, count) => {
        plan.joinedCount = count ? count : 0;
        rep({ code: 200, data: plan });
      });
    } else {
      rep({
        code: 404,
        msg: "Plan not found"
      });
    }
  });
});

server.call("increaseJoinedCount", allowall, "增加已加入车辆数量", "", (ctx: ServerContext, rep: ((result: any) => void), pid: string) => {
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
        msg: err.message
      });
    } else {
      rep({ code: 200, data: count });
    }
  });
});

server.call("decreaseJoinedCount", allowall, "减少已加入车辆数", "", (ctx: ServerContext, rep: ((result: any) => void), pid: string) => {
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
        msg: err.message
      });
    } else {
      rep({ code: 200, data: count });
    }
  });
});

server.call("setJoinedCounts", allowall, "设置计划加入车辆数", "可以以数组的方式批量设置计划加入的车辆数。", (ctx: ServerContext, rep: ((result: any) => void), params: [string, number][]) => {
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
        msg: err.message
      });
    } else {
      rep({
        code: 200,
        data: "SUCCESS"
      });
    }
  });
});

server.call("refresh", allowall, "", "", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("refresh uid: %s", ctx.uid);
  const pkt: CmdPacket = { cmd: "Refresh", args: [] };
  ctx.publish(pkt);
  rep({ code: 200, data: "okay"});
});

function ids2plans(ctx: ServerContext, ids: string[], rep: ((result: any) => void)) {
  let multi = ctx.cache.multi();
  for (let id of ids) {
    multi.hget("plan-entities", id);
  }
  multi.exec(function(err, planstrs) {
    if (err) {
      rep({ code: 500, msg: err.message });
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
        rep({ code: 200, data: plans });
      });
    }
  });
}